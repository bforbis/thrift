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

import { TransportState, OnFlushCallback } from '../thrift';
import TAbstractTransport from './abstract_transport';

export default class TBufferedTransport extends TAbstractTransport {
  public defaultReadBufferSize: number;
  public writeBufferSize: number;
  public writeCursor: number = 0;

  public constructor(buffer?: Buffer, callback?: OnFlushCallback) {
    super(buffer, callback);
    this.defaultReadBufferSize = 1024;
    this.writeBufferSize = 512; // Soft Limit
    this.reset();
  }

  public reset(): void {
    super.reset();
    this.inBuf = Buffer.alloc(this.defaultReadBufferSize);
    this.writeCursor = 0;
  }

  public commitPosition(): void {
    const unreadSize = this.writeCursor - this.readCursor;
    const bufSize =
      unreadSize * 2 > this.defaultReadBufferSize
        ? unreadSize * 2
        : this.defaultReadBufferSize;
    const buf = Buffer.alloc(bufSize);
    if (unreadSize > 0) {
      this.inBuf.copy(buf, 0, this.readCursor, this.writeCursor);
    }
    this.readCursor = 0;
    this.writeCursor = unreadSize;
    this.inBuf = buf;
  }

  public rollbackPosition(): void {
    this.readCursor = 0;
  }

  public borrow(): TransportState {
    return {
      buf: this.inBuf,
      readIndex: this.readCursor,
      writeIndex: this.writeCursor
    };
  }

  public flush(): void {
    // If the seqid of the callback is available pass it to the onFlush
    // Then remove the current seqid
    const seqid = this._seqid;
    this._seqid = null;

    if (this.outCount < 1) {
      return;
    }

    const msg = new Buffer(this.outCount);
    let pos = 0;

    this.outBuffers.forEach(buf => {
      buf.copy(msg, pos, 0);
      pos += buf.length;
    });

    if (this.onFlush) {
      // Passing seqid through this call to get it to the connection
      this.onFlush(msg, seqid);
    }

    this.outBuffers = [];
    this.outCount = 0;
  }

  public static receiver(
    callback: Function,
    seqid: number
  ): (data: Buffer) => void {
    const reader = new TBufferedTransport();

    return function(data) {
      if (reader.writeCursor + data.length > reader.inBuf.length) {
        const buf = new Buffer(reader.writeCursor + data.length);
        reader.inBuf.copy(buf, 0, 0, reader.writeCursor);
        reader.inBuf = buf;
      }
      data.copy(reader.inBuf, reader.writeCursor, 0);
      reader.writeCursor += data.length;

      callback(reader, seqid);
    };
  }
}
