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

/* eslint-env browser */

import { EventEmitter } from 'events';
import {
  TConnection,
  StaticClass,
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
import createClient from './create_client';

/**
 * Constructor Function for the XHR Connection.
 * If you do not specify a host and port then XHRConnection will default to the
 * host and port of the page from which this javascript is served.
 * @constructor
 * @param {string} [url] - The URL to connect to.
 * @classdesc TXHRConnection objects provide Thrift end point transport
 *     semantics implemented using XHR.
 * @example
 *     var transport = new Thrift.TXHRConnection('localhost', 9099, {});
 */

interface XHRConnectOptions {
  transport?: TTransportClass<TAbstractTransport>;
  protocol?: StaticClass<TAbstractProtocol>;
  useCORS?: boolean;
  https?: boolean;
  path?: string;
  [key: string]: any;
}

export class XHRConnection extends EventEmitter implements TConnection {
  public url: string;
  public options: XHRConnectOptions;
  public useCORS: boolean;
  public wpos = 0;
  public rpos = 0;
  public send_buf = '';
  public recv_buf: string = '';
  private recv_buf_sz: number = 0;
  public transport: TTransportClass<TAbstractTransport>;
  public protocol: StaticClass<TAbstractProtocol>;
  public headers: { [key: string]: string } = {};
  //The sequence map is used to map seqIDs back to the
  //  calling client in multiplexed scenarios
  public seqId2Service: { [key: number]: string } = {};

  // Set in create_client
  public client?: ServiceClient | MultiplexedServiceClient;

  public constructor(
    host: string,
    port?: number | string,
    options: XHRConnectOptions = {}
  ) {
    super();
    this.options = options;
    this.useCORS = !!(options && options.useCORS);
    this.transport = options.transport || TBufferedTransport;
    this.protocol = options.protocol || TJSONProtocol;
    this.headers = options.headers || {};

    host = host || window.location.host;
    port = port || window.location.port;

    const prefix = options.https ? 'https://' : 'http://';
    const path = options.path || '/';

    if (port === '') {
      port = undefined;
    }

    if (!port || port === 80 || port === '80') {
      this.url = prefix + host + path;
    } else {
      this.url = prefix + host + ':' + port + path;
    }
  }

  /**
   * Gets the browser specific XmlHttpRequest Object.
   * @returns {object} the browser XHR interface object
   */
  public getXmlHttpRequestObject(): XMLHttpRequest {
    /* eslint-disable no-empty */
    try {
      return new XMLHttpRequest();
    } catch (e1) {}
    try {
      // eslint-disable-next-line no-undef
      return new ActiveXObject('Msxml2.XMLHTTP');
    } catch (e2) {}
    try {
      // eslint-disable-next-line no-undef
      return new ActiveXObject('Microsoft.XMLHTTP');
    } catch (e3) {}
    /* eslint-enable no-empty */
    throw "Your browser doesn't support XHR.";
  }

  /**
   * Sends the current XRH request if the transport was created with a URL
   * and the async parameter is false. If the transport was not created with
   * a URL, or the async parameter is True and no callback is provided, or
   * the URL is an empty string, the current send buffer is returned.
   * @param {object} async - If true the current send buffer is returned.
   * @param {object} callback - Optional async completion callback
   * @returns {undefined|string} Nothing or the current send buffer.
   * @throws {string} If XHR fails.
   */
  public flush(): string | undefined {
    const self = this;
    if (this.url === undefined || this.url === '') {
      return this.send_buf;
    }

    const xreq = this.getXmlHttpRequestObject();

    if (xreq.overrideMimeType) {
      xreq.overrideMimeType('application/json');
    }

    xreq.onreadystatechange = function() {
      if (this.readyState == 4 && this.status == 200) {
        self.setRecvBuffer(this.responseText);
      }
    };

    xreq.open('POST', this.url, true);

    Object.keys(this.headers).forEach(function(headerKey) {
      xreq.setRequestHeader(headerKey, self.headers[headerKey]);
    });

    xreq.send(this.send_buf);
  }

  /**
   * Sets the buffer to provide the protocol when deserializing.
   * @param {string} buf - The buffer to supply the protocol.
   */
  public setRecvBuffer(buf: string | ArrayBuffer): void {
    this.recv_buf = buf as string;
    this.rpos = 0;

    let data: Buffer;

    if (typeof buf === 'string') {
      data = Buffer.from(buf);
      this.recv_buf_sz = buf.length;
      this.wpos = buf.length;
    } else if (buf instanceof ArrayBuffer) {
      this.recv_buf_sz = buf.byteLength;
      this.wpos = buf.byteLength;
      data = Buffer.from(buf);
    } else {
      data = Buffer.alloc(0);
    }

    this.transport.receiver(this.__decodeCallback.bind(this), 0)(data);
  }

  public __decodeCallback(transport_with_data: TAbstractTransport): void {
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

  /**
   * Returns true if the transport is open, XHR always returns true.
   * @readonly
   * @returns {boolean} Always True.
   */
  public isOpen(): boolean {
    return true;
  }

  /**
   * Opens the transport connection, with XHR this is a nop.
   */
  public open(): void {}

  /**
   * Closes the transport connection, with XHR this is a nop.
   */
  public close(): void {}

  /**
   * Returns the specified number of characters from the response
   * buffer.
   * @param {number} len - The number of characters to return.
   * @returns {string} Characters sent by the server.
   */
  public read(len: number): string {
    const avail = this.wpos - this.rpos;

    if (avail === 0) {
      return '';
    }

    let give = len;

    if (avail < len) {
      give = avail;
    }

    const ret = this.recv_buf.substr(this.rpos, give);
    this.rpos += give;

    //clear buf when complete?
    return ret;
  }

  /**
   * Returns the entire response buffer.
   * @returns {string} Characters sent by the server.
   */
  public readAll(): string {
    return this.recv_buf;
  }

  /**
   * Sets the send buffer to buf.
   * @param {string} buf - The buffer to send.
   */
  public write(buf: Buffer | string): void {
    this.send_buf = buf as string;
    this.flush();
  }

  /**
   * Returns the send buffer.
   * @readonly
   * @returns {string} The send buffer.
   */
  public getSendBuffer(): string {
    return this.send_buf;
  }
}

/**
 * Creates a new TXHRTransport object, used by Thrift clients to connect
 *    to Thrift HTTP based servers.
 * @param {string} host - The host name or IP to connect to.
 * @param {number} port - The TCP port to connect to.
 * @param {XHRConnectOptions} options - The configuration options to use.
 * @returns {XHRConnection} The connection object.
 * @see {@link XHRConnectOptions}
 */
export function createXHRConnection(
  host: string,
  port: number,
  options?: XHRConnectOptions
): XHRConnection {
  return new XHRConnection(host, port, options);
}

export { createClient as createXHRClient };
