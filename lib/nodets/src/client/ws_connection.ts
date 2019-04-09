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

import WebSocket = require('ws');
import { EventEmitter } from 'events';
import {
  StaticClass,
  TConnection,
  ServiceClient,
  MultiplexedServiceClient
} from '../thrift';
import {
  TBufferedTransport,
  TTransportClass,
  TAbstractTransport
} from '../transport';
import { TJSONProtocol, TAbstractProtocol } from '../protocol';
import {
  InputBufferUnderrunError,
  TApplicationException,
  TApplicationExceptionType
} from '../exception';
import { default as createClient } from './create_client';

/**
 * @class
 * @name WSConnectOptions
 * @property {string} transport - The Thrift layered transport to use (TBufferedTransport, etc).
 * @property {string} protocol - The Thrift serialization protocol to use (TJSONProtocol, etc.).
 * @property {string} path - The URL path to connect to (e.g. "/", "/mySvc", "/thrift/quoteSvc", etc.).
 * @property {object} headers - A standard Node.js header hash, an object hash containing key/value
 *        pairs where the key is the header name string and the value is the header value string.
 * @property {boolean} secure - True causes the connection to use wss, otherwise ws is used.
 * @property {object} wsOptions - Options passed on to WebSocket.
 * @example
 *     //Use a secured websocket connection
 *     //  uses the buffered transport layer, uses the JSON protocol and directs RPC traffic
 *     //  to wss://thrift.example.com:9090/hello
 *     var thrift = require('thrift');
 *     var options = {
 *        transport: thrift.TBufferedTransport,
 *        protocol: thrift.TJSONProtocol,
 *        path: "/hello",
 *        secure: true
 *     };
 *     var con = thrift.createWSConnection("thrift.example.com", 9090, options);
 *     con.open()
 *     var client = thrift.createWSClient(myService, connection);
 *     client.myServiceFunction();
 *     con.close()
 */

/**
 * Initializes a Thrift WSConnection instance (use createWSConnection() rather than
 *    instantiating directly).
 * @constructor
 * @param {string} host - The host name or IP to connect to.
 * @param {number} port - The TCP port to connect to.
 * @param {WSConnectOptions} options - The configuration options to use.
 * @throws {error} Exceptions other than ttransport.InputBufferUnderrunError are rethrown
 * @event {error} The "error" event is fired when a Node.js error event occurs during
 *     request or response processing, in which case the node error is passed on. An "error"
 *     event may also be fired when the connectison can not map a response back to the
 *     appropriate client (an internal error), generating a TApplicationException.
 * @classdesc WSConnection objects provide Thrift end point transport
 *     semantics implemented using Websockets.
 * @see {@link createWSConnection}
 */

interface WSConnectionOptions {
  path?: string;
  transport?: TTransportClass<TAbstractTransport>;
  protocol?: StaticClass<TAbstractProtocol>;
  headers?: {};
  [key: string]: any;
}
export class WSConnection extends EventEmitter implements TConnection {
  public host: string;
  public options: WSConnectionOptions;
  public port: number;
  public secure: boolean;
  public transport: TTransportClass<TAbstractTransport>;
  public protocol: StaticClass<TAbstractProtocol>;
  public path?: string;
  public send_pending: Buffer[];
  public wsOptions: {
    host: string;
    port: number;
    path: string;
    headers: {};
    [key: string]: any;
  };
  public seqId2Service: { [key: number]: string } = {};
  public socket: WebSocket | null = null;
  public client?: ServiceClient | MultiplexedServiceClient;

  public constructor(
    host: string,
    port: number,
    options?: WSConnectionOptions
  ) {
    super();
    //Set configuration

    this.options = options || {};
    this.host = host;
    this.port = port;
    this.secure = this.options.secure || false;
    this.transport = this.options.transport || TBufferedTransport;
    this.protocol = this.options.protocol || TJSONProtocol;
    this.path = this.options.path;
    this.send_pending = [];

    //The sequence map is used to map seqIDs back to the
    //  calling client in multiplexed scenarios
    this.seqId2Service = {};

    //Prepare WebSocket options
    this.wsOptions = {
      host: this.host,
      port: this.port || 80,
      path: this.options.path || '/',
      headers: this.options.headers || {}
    };
    for (const attrname in this.options.wsOptions) {
      this.wsOptions[attrname] = this.options.wsOptions[attrname];
    }
  }

  private __reset(): void {
    this.socket = null; //The web socket
    this.send_pending = []; //Buffers/Callback pairs waiting to be sent
  }

  private __onOpen(): void {
    const self = this;
    this.emit('open');
    if (this.send_pending.length > 0) {
      //If the user made calls before the connection was fully
      //open, send them now
      this.send_pending.forEach(function(data) {
        if (self.socket) {
          self.socket.send(data);
        }
      });
      this.send_pending = [];
    }
  }

  private __onClose(): void {
    this.emit('close');
    this.__reset();
  }

  private __decodeCallback(transport_with_data: TAbstractTransport): void {
    const proto = new this.protocol(transport_with_data);
    try {
      while (true) {
        const header = proto.readMessageBegin();
        const dummy_seqid = header.rseqid * -1;
        if (this.client) {
          let client: ServiceClient;
          //The Multiplexed Protocol stores a hash of seqid to service names
          //  in seqId2Service. If the SeqId is found in the hash we need to
          //  lookup the appropriate client for this call.
          //  The client var is a single client object when not multiplexing,
          //  when using multiplexing it is a service name keyed hash of client
          //  objects.
          //NOTE: The 2 way interdependencies between protocols, transports,
          //  connections and clients in the Node.js implementation are irregular
          //  and make the implementation difficult to extend and maintain. We
          //  should bring this stuff inline with typical thrift I/O stack
          //  operation soon.
          //  --ra
          const service_name = this.seqId2Service[header.rseqid];
          if (service_name) {
            client = this.client[service_name];
            delete this.seqId2Service[header.rseqid];
          } else {
            client = this.client as ServiceClient;
          }
          /*jshint -W083 */
          client._reqs[dummy_seqid] = function(err, success) {
            transport_with_data.commitPosition();
            const clientCallback = client._reqs[header.rseqid];
            delete client._reqs[header.rseqid];
            if (clientCallback) {
              clientCallback(err, success);
            }
          };
          /*jshint +W083 */
          if (client['recv_' + header.fname]) {
            client['recv_' + header.fname](proto, header.mtype, dummy_seqid);
          } else {
            delete client._reqs[dummy_seqid];
            this.emit(
              'error',
              new TApplicationException(
                TApplicationExceptionType.WRONG_METHOD_NAME,
                'Received a response to an unknown RPC function'
              )
            );
          }
        }
      }
    } catch (e) {
      if (e instanceof InputBufferUnderrunError) {
        transport_with_data.rollbackPosition();
      } else {
        throw e;
      }
    }
  }

  private __onData(data: WebSocket.Data): void {
    let buf: Buffer;
    if (data instanceof Array) {
      buf = Buffer.concat(data);
    } else if (typeof data === 'string') {
      buf = Buffer.from(data);
    } else if (data instanceof ArrayBuffer) {
      buf = Buffer.from(new Uint8Array(data));
    } else {
      buf = data;
    }
    this.transport.receiver(this.__decodeCallback.bind(this), 0)(buf);
  }

  private __onMessage(evt: { data: WebSocket.Data }): void {
    this.__onData(evt.data);
  }

  private __onError(evt: { error: any; message: string; type: string }): void {
    this.emit('error', evt);
    if (this.socket) {
      this.socket.close();
    }
  }

  /**
   * Returns true if the transport is open
   * @readonly
   * @returns {boolean}
   */
  public isOpen(): boolean {
    return this.socket != null && this.socket.readyState == this.socket.OPEN;
  }

  /**
   * Opens the transport connection
   */
  public open(): void {
    //If OPEN/CONNECTING/CLOSING ignore additional opens
    if (this.socket && this.socket.readyState != this.socket.CLOSED) {
      return;
    }
    //If there is no socket or the socket is closed:
    this.socket = new WebSocket(this.uri(), '', this.wsOptions);
    this.socket.binaryType = 'arraybuffer';
    this.socket.onopen = this.__onOpen.bind(this);
    this.socket.onmessage = this.__onMessage.bind(this);
    this.socket.onerror = this.__onError.bind(this);
    this.socket.onclose = this.__onClose.bind(this);
  }

  /**
   * Closes the transport connection
   */
  public close(): void {
    if (this.socket) {
      this.socket.close();
    }
  }

  /**
   * Return URI for the connection
   * @returns {string} URI
   */
  public uri(): string {
    const schema = this.secure ? 'wss' : 'ws';
    let port = '';
    const path = this.path || '/';
    const host = this.host;

    // avoid port if default for schema
    if (
      this.port &&
      (('wss' == schema && this.port != 443) ||
        ('ws' == schema && this.port != 80))
    ) {
      port = ':' + this.port;
    }

    return `${schema}://${host}${port}${path}`;
  }

  /**
   * Writes Thrift message data to the connection
   * @param {Buffer} data - A Node.js Buffer containing the data to write
   * @returns {void} No return value.
   * @event {error} the "error" event is raised upon request failure passing the
   *     Node.js error object to the listener.
   */
  public write(data: Buffer): void {
    if (this.socket && this.isOpen()) {
      //Send data and register a callback to invoke the client callback
      this.socket.send(data);
    } else {
      //Queue the send to go out __onOpen
      this.send_pending.push(data);
    }
  }
}

/**
 * Creates a new WSConnection object, used by Thrift clients to connect
 *    to Thrift HTTP based servers.
 * @param {string} host - The host name or IP to connect to.
 * @param {number} port - The TCP port to connect to.
 * @param {WSConnectOptions} options - The configuration options to use.
 * @returns {WSConnection} The connection object.
 * @see {@link WSConnectOptions}
 */
export function createWSConnection(
  host: string,
  port: number,
  options?: WSConnectionOptions
): WSConnection {
  return new WSConnection(host, port, options);
}

export { createClient as createWSClient };
