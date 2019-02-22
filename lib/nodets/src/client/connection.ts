/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import * as child_process from 'child_process';
import * as net from 'net';
import * as tls from 'tls';
import * as stream from 'stream';
import { EventEmitter } from 'events';
import { SSL_OP_NO_SSLv2, SSL_OP_NO_SSLv3 } from 'constants';

import {
  TAbstractTransport,
  TTransportClass,
  TBufferedTransport
} from '../transport';
import {
  TAbstractProtocol,
  TProtocolClass,
  TBinaryProtocol
} from '../protocol';
import {
  InputBufferUnderrunError,
  TApplicationException,
  TApplicationExceptionType
} from '../exception';
import * as log from '../util/log';
import { ServiceClient, TConnection } from '../thrift';

export { default as createClient } from './create_client';
export { default as createStdIOClient } from './create_client';

interface ConnectionOptions<
  TTransport extends TAbstractTransport,
  TProtocol extends TAbstractProtocol
> extends tls.ConnectionOptions {
  transport?: TTransportClass<TTransport>;
  protocol?: TProtocolClass<TProtocol>;
  debug?: boolean;
  max_attempts?: number;
  retry_max_delay?: number;
  connect_timeout?: number;
  timeout?: number;
}

export class Connection<
  TTransport extends TAbstractTransport,
  TProtocol extends TAbstractProtocol
> extends EventEmitter implements TConnection {
  public seqId2Service: { [key: number]: string } = {};
  public connection: net.Socket | tls.TLSSocket;
  public host?: string;
  public port?: number;
  public path?: string;
  public ssl: boolean;
  public options: ConnectionOptions<TTransport, TProtocol>;
  public transport: TTransportClass<TTransport>;
  public protocol: TProtocolClass<TProtocol>;
  public offline_queue: any[] = [];
  public connected: boolean = false;
  public _debug: boolean;
  public max_attempts?: number;
  public retry_max_delay: null | number = null;
  public connect_timeout: boolean | number = false;

  // Set in create_client.ts
  public client?: ServiceClient;

  // Retry attributes
  public retry_timer: null | NodeJS.Timeout = null;
  public retry_totaltime = 0;
  public retry_delay = 150;
  public retry_backoff = 1.7;
  public attempts = 0;

  public constructor(
    stream: net.Socket | tls.TLSSocket,
    options?: ConnectionOptions<TTransport, TProtocol>
  ) {
    super();
    const self = this;
    this.connection = stream;
    this.ssl = !!(stream as tls.TLSSocket).encrypted;
    this.options = options || {};

    // Default to TBufferedTransport/TBinaryProtocol if no transport/protocol
    // class is provided. Need to downcast/upcast to get class definitions to
    // play nicely with generic class parameters
    this.transport =
      this.options.transport ||
      ((TBufferedTransport as unknown) as TTransportClass<TTransport>);

    this.protocol =
      this.options.protocol ||
      ((TBinaryProtocol as unknown) as TProtocolClass<TProtocol>);

    this.initialize_retry_vars();

    this._debug = this.options.debug || false;
    if (
      this.options.max_attempts &&
      !isNaN(this.options.max_attempts) &&
      this.options.max_attempts > 0
    ) {
      this.max_attempts = +this.options.max_attempts;
    }
    if (
      this.options.retry_max_delay !== undefined &&
      !isNaN(this.options.retry_max_delay) &&
      this.options.retry_max_delay > 0
    ) {
      this.retry_max_delay = this.options.retry_max_delay;
    }

    if (
      this.options.connect_timeout &&
      !isNaN(this.options.connect_timeout) &&
      this.options.connect_timeout > 0
    ) {
      this.connect_timeout = +this.options.connect_timeout;
    }

    this.connection.addListener(
      this.ssl ? 'secureConnect' : 'connect',
      function() {
        self.connected = true;

        this.setTimeout(self.options.timeout || 0);
        this.setNoDelay();
        this.frameLeft = 0;
        this.framePos = 0;
        this.frame = null;
        self.initialize_retry_vars();

        self.offline_queue.forEach(function(data) {
          self.connection.write(data);
        });

        self.emit('connect');
      }
    );

    this.connection.addListener('error', function(err) {
      // Only emit the error if no-one else is listening on the connection
      // or if someone is listening on us, because Node turns unhandled
      // 'error' events into exceptions.
      if (
        self.connection.listeners('error').length === 1 ||
        self.listeners('error').length > 0
      ) {
        self.emit('error', err);
      }
    });

    // Add a close listener
    this.connection.addListener('close', function() {
      self.connection_gone(); // handle close event. try to reconnect
    });

    this.connection.addListener('timeout', function() {
      self.emit('timeout');
    });

    this.connection.addListener(
      'data',
      self.transport.receiver(function(transport_with_data) {
        const message = new self.protocol(transport_with_data);
        try {
          while (true) {
            const header = message.readMessageBegin();
            const dummy_seqid = header.rseqid * -1;
            let client = self.client;
            //The Multiplexed Protocol stores a hash of seqid to service names
            //  in seqId2Service. If the SeqId is found in the hash we need to
            //  lookup the appropriate client for this call.
            //  The connection.client object is a single client object when not
            //  multiplexing, when using multiplexing it is a service name keyed
            //  hash of client objects.
            //NOTE: The 2 way interdependencies between protocols, transports,
            //  connections and clients in the Node.js implementation are irregular
            //  and make the implementation difficult to extend and maintain. We
            //  should bring this stuff inline with typical thrift I/O stack
            //  operation soon.
            //  --ra
            const service_name = self.seqId2Service[header.rseqid];
            if (service_name) {
              client = self.client[service_name];
            }

            client._reqs[dummy_seqid] = function(err, success) {
              transport_with_data.commitPosition();

              const callback = client._reqs[header.rseqid];
              delete client._reqs[header.rseqid];
              if (service_name) {
                delete self.seqId2Service[header.rseqid];
              }
              if (callback) {
                callback(err, success);
              }
            };

            if (client['recv_' + header.fname]) {
              client['recv_' + header.fname](
                message,
                header.mtype,
                dummy_seqid
              );
            } else {
              delete client._reqs[dummy_seqid];
              self.emit(
                'error',
                new TApplicationException(
                  TApplicationExceptionType.WRONG_METHOD_NAME,
                  'Received a response to an unknown RPC function'
                )
              );
            }
          }
        } catch (e) {
          if (e instanceof InputBufferUnderrunError) {
            transport_with_data.rollbackPosition();
          } else {
            self.emit('error', e);
          }
        }
      }, 0)
    );
  }

  public end(): void {
    this.connection.end();
  }

  public destroy(): void {
    this.connection.destroy();
  }

  public initialize_retry_vars(): void {
    this.retry_timer = null;
    this.retry_totaltime = 0;
    this.retry_delay = 150;
    this.retry_backoff = 1.7;
    this.attempts = 0;
  }

  public write(data: Buffer): void {
    if (!this.connected) {
      this.offline_queue.push(data);
      return;
    }
    this.connection.write(data);
  }

  public connection_gone(): void {
    const self = this;
    this.connected = false;

    // If a retry is already in progress, just let that happen
    if (this.retry_timer) {
      return;
    }
    // We cannot reconnect a secure socket.
    if (!this.max_attempts || this.ssl) {
      self.emit('close');
      return;
    }

    if (
      this.retry_max_delay !== null &&
      this.retry_delay >= this.retry_max_delay
    ) {
      this.retry_delay = this.retry_max_delay;
    } else {
      this.retry_delay = Math.floor(this.retry_delay * this.retry_backoff);
    }

    log.debug('Retry connection in ' + this.retry_delay + ' ms');

    if (this.max_attempts && this.attempts >= this.max_attempts) {
      this.retry_timer = null;
      console.error(
        "thrift: Couldn't get thrift connection after " +
          this.max_attempts +
          ' attempts.'
      );
      self.emit('close');
      return;
    }

    this.attempts += 1;
    this.emit('reconnecting', {
      delay: self.retry_delay,
      attempt: self.attempts
    });

    this.retry_timer = setTimeout(function() {
      if (self.connection.destroyed) {
        self.retry_timer = null;
        return;
      }

      log.debug('Retrying connection...');

      self.retry_totaltime += self.retry_delay;

      if (
        self.connect_timeout &&
        self.retry_totaltime >= self.connect_timeout
      ) {
        self.retry_timer = null;
        console.error(
          "thrift: Couldn't get thrift connection after " +
            self.retry_totaltime +
            'ms.'
        );
        self.emit('close');
        return;
      }

      if (self.path !== undefined) {
        self.connection.connect(self.path);
      } else {
        self.connection.connect(self.port, self.host);
      }
      self.retry_timer = null;
    }, this.retry_delay);
  }
}

export function createConnection<
  TTransport extends TAbstractTransport,
  TProtocol extends TAbstractProtocol
>(
  host: string,
  port: number,
  options?: ConnectionOptions<TTransport, TProtocol>
): Connection<TTransport, TProtocol> {
  const stream = net.createConnection(port, host);
  const connection = new Connection(stream, options);
  connection.host = host;
  connection.port = port;

  return connection;
}

export function createUDSConnection<
  TTransport extends TAbstractTransport,
  TProtocol extends TAbstractProtocol
>(
  path: string,
  options: ConnectionOptions<TTransport, TProtocol>
): Connection<TTransport, TProtocol> {
  const stream = net.createConnection(path);
  const connection = new Connection(stream, options);
  connection.path = path;

  return connection;
}

export function createSSLConnection<
  TTransport extends TAbstractTransport,
  TProtocol extends TAbstractProtocol
>(
  host: string,
  port: number,
  options?: ConnectionOptions<TTransport, TProtocol>
): Connection<TTransport, TProtocol> {
  if (!('secureProtocol' in options) && !('secureOptions' in options)) {
    options.secureProtocol = 'SSLv23_method';
    options.secureOptions = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3;
  }

  const stream = tls.connect(port, host, options);
  const connection = new Connection(stream, options);
  connection.host = host;
  connection.port = port;

  return connection;
}

interface StdIOConnectionOptions<
  TTransport extends TAbstractTransport,
  TProtocol extends TAbstractProtocol
> {
  transport?: TTransportClass<TTransport>;
  protocol?: TProtocolClass<TProtocol>;
}

export class StdIOConnection<
  TTransport extends TAbstractTransport,
  TProtocol extends TAbstractProtocol
> extends EventEmitter implements TConnection {
  public options?: StdIOConnectionOptions<TTransport, TProtocol>;

  public frameLeft = 0;
  public framePos = 0;
  public frame = null;
  public connected = true;
  public child: child_process.ChildProcess;
  public connection: stream.Writable;
  public transport: TTransportClass<TTransport>;
  public protocol: TProtocolClass<TProtocol>;
  public offline_queue: any[] = [];

  public client?: ServiceClient;

  public constructor(
    command: string,
    options?: StdIOConnectionOptions<TTransport, TProtocol>
  ) {
    super();

    const command_parts = command.split(' ');
    command = command_parts[0];
    const args = command_parts.splice(1, command_parts.length - 1);
    const child = (this.child = child_process.spawn(command, args));

    const self = this;
    this.connection = child.stdin;
    this.options = options || {};

    // Default to TBufferedTransport/TBinaryProtocol if no transport/protocol
    // class is provided. Need to downcast/upcast to get class definitions to
    // play nicely with generic class parameters
    this.transport =
      this.options.transport ||
      ((TBufferedTransport as unknown) as TTransportClass<TTransport>);

    this.protocol =
      this.options.protocol ||
      ((TBinaryProtocol as unknown) as TProtocolClass<TProtocol>);

    if (log.getLogLevel() === 'debug') {
      this.child.stderr.on('data', function(err) {
        log.debug(err.toString(), 'CHILD ERROR');
      });

      this.child.on('exit', function(code, signal) {
        log.debug(code + ':' + signal, 'CHILD EXITED');
      });
    }

    self.offline_queue.forEach(function(data) {
      self.connection.write(data);
    });

    this.connection.addListener('error', function(err) {
      self.emit('error', err);
    });

    // Add a close listener
    this.connection.addListener('close', function() {
      self.emit('close');
    });

    child.stdout.addListener(
      'data',
      self.transport.receiver(function(transport_with_data) {
        const message = new self.protocol(transport_with_data);
        try {
          const header = message.readMessageBegin();
          const dummy_seqid = header.rseqid * -1;
          const client = self.client;
          client._reqs[dummy_seqid] = function(err, success) {
            transport_with_data.commitPosition();

            const callback = client._reqs[header.rseqid];
            delete client._reqs[header.rseqid];
            if (callback) {
              callback(err, success);
            }
          };
          client['recv_' + header.fname](message, header.mtype, dummy_seqid);
        } catch (e) {
          if (e instanceof InputBufferUnderrunError) {
            transport_with_data.rollbackPosition();
          } else {
            throw e;
          }
        }
      }, 0)
    );
  }
  public end(): void {
    this.connection.end();
  }

  public write(data: Buffer): void {
    if (!this.connected) {
      this.offline_queue.push(data);
      return;
    }
    this.connection.write(data);
  }
}

export function createStdIOConnection<
  TTransport extends TAbstractTransport,
  TProtocol extends TAbstractProtocol
>(
  command: string,
  options?: StdIOConnectionOptions<TTransport, TProtocol>
): StdIOConnection<TTransport, TProtocol> {
  return new StdIOConnection(command, options);
}
