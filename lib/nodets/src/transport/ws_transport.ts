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

import * as log from '../util/log';
import * as WebSocket from 'ws';

export default class TWebSocketTransport {
  public url: string; //Where to connect
  public socket: null | WebSocket; //The web socket
  private callbacks: Function[]; //Pending callbacks
  private send_pending: any[]; //Buffers/Callback pairs waiting to be sent
  private send_buf: string; //Outbound data, immutable until sent
  private recv_buf: string; //Inbound data
  private rb_wpos: number; //Network write position in receive buffer
  private rb_rpos: number; //Client read position in receive buffer
  private wpos: number; // Write position
  private rpos: number; // Read position

  /**
   * Constructor Function for the WebSocket transport.
   * @constructor
   * @param {string} [url] - The URL to connect to.
   * @classdesc The Apache Thrift Transport layer performs byte level I/O
   * between RPC clients and servers. The JavaScript TWebSocketTransport object
   * uses the WebSocket protocol. Target servers must implement WebSocket.
   * (see: node.js example server_http.js).
   * @example
   *   var transport = new Thrift.TWebSocketTransport("http://localhost:8585");
   */
  public constructor(url: string) {
    this.__reset(url);
  }

  private __reset(url: string): void {
    this.url = url;
    this.socket = null;
    this.callbacks = [];
    this.send_pending = [];
    this.send_buf = '';
    this.recv_buf = '';
    this.rb_wpos = 0;
    this.rb_rpos = 0;
    this.wpos = 0;
    this.rpos = 0;
  }

  /**
   * Sends the current WS request and registers callback. The async
   * parameter is ignored (WS flush is always async) and the callback
   * function parameter is required.
   * @param {object} async - Ignored.
   * @param {object} callback - The client completion callback.
   * @returns {undefined|string} Nothing (undefined)
   */
  public flush(async: any, callback: Function): void {
    const self = this;
    if (this.isOpen()) {
      //Send data and register a callback to invoke the client callback
      this.socket.send(this.send_buf);
      this.callbacks.push(
        (function() {
          const clientCallback = callback;
          return function(msg: string) {
            self.setRecvBuffer(msg);
            clientCallback();
          };
        })()
      );
    } else {
      //Queue the send to go out __onOpen
      this.send_pending.push({
        buf: this.send_buf,
        cb: callback
      });
    }
  }

  private __onOpen(): void {
    const self = this;
    if (this.send_pending.length > 0) {
      //If the user made calls before the connection was fully
      //open, send them now
      this.send_pending.forEach(function(elem) {
        this.socket.send(elem.buf);
        this.callbacks.push(
          (function() {
            const clientCallback = elem.cb;
            return function(msg: string) {
              self.setRecvBuffer(msg);
              clientCallback();
            };
          })()
        );
      });
      this.send_pending = [];
    }
  }

  private __onClose(evt): void {
    this.__reset(this.url);
  }

  private __onMessage(evt): void {
    if (this.callbacks.length) {
      this.callbacks.shift()(evt.data);
    }
  }

  private __onError(evt): void {
    log.error('websocket: ' + evt.toString());
    this.socket.close();
  }

  /**
   * Sets the buffer to use when receiving server responses.
   * @param {string} buf - The buffer to receive server responses.
   */
  public setRecvBuffer(buf: string): void {
    this.recv_buf = buf;
    this.wpos = this.recv_buf.length;
    this.rpos = 0;
  }

  /**
   * Returns true if the transport is open
   * @readonly
   * @returns {boolean}
   */
  public isOpen(): boolean {
    return this.socket !== null && this.socket.readyState == this.socket.OPEN;
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
    this.socket = new WebSocket(this.url);
    this.socket.onopen = this.__onOpen.bind(this);
    this.socket.onmessage = this.__onMessage.bind(this);
    this.socket.onerror = this.__onError.bind(this);
    this.socket.onclose = this.__onClose.bind(this);
  }

  /**
   * Closes the transport connection
   */
  public close(): void {
    this.socket.close();
  }

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
  public write(buf: string): void {
    this.send_buf = buf;
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
