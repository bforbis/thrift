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

import * as binary from '../util/binary';
import { OnFlushCallback, TransportState } from '../thrift';
import TAbstractTransport from './abstract_transport';

export default class TFramedTransport extends TAbstractTransport {
  public constructor(buffer?: Buffer, callback?: OnFlushCallback) {
    super(buffer, callback);
  }

  public commitPosition(): void {}
  public rollbackPosition(): void {}
  public borrow(): TransportState {
    return {
      buf: this.inBuf,
      readIndex: this.readCursor,
      writeIndex: this.inBuf.length
    };
  }

  public flush(): void {
    // If the seqid of the callback is available pass it to the onFlush
    // Then remove the current seqid
    const seqid = this._seqid;
    this._seqid = null;

    const out = Buffer.alloc(this.outCount);
    let pos = 0;
    this.outBuffers.forEach(function(buf) {
      buf.copy(out, pos, 0);
      pos += buf.length;
    });

    if (this.onFlush) {
      // TODO: optimize this better, allocate one buffer instead of both:
      const msg = Buffer.alloc(out.length + 4);
      binary.writeI32(msg, out.length);
      out.copy(msg, 4, 0, out.length);
      if (this.onFlush) {
        // Passing seqid through this call to get it to the connection
        this.onFlush(msg, seqid);
      }
    }

    this.outBuffers = [];
    this.outCount = 0;
  }

  public static receiver(
    callback: Function,
    seqid: number
  ): (data: Buffer) => void {
    let residual: Buffer | null = null;

    return function(data: Buffer) {
      // Prepend any residual data from our previous read
      if (residual) {
        data = Buffer.concat([residual, data]);
        residual = null;
      }

      // framed transport
      while (data.length) {
        if (data.length < 4) {
          // Not enough bytes to continue, save and resume on next packet
          residual = data;
          return;
        }
        const frameSize = binary.readI32(data, 0);
        if (data.length < 4 + frameSize) {
          // Not enough bytes to continue, save and resume on next packet
          residual = data;
          return;
        }

        const frame = data.slice(4, 4 + frameSize);
        residual = data.slice(4 + frameSize);

        callback(new TFramedTransport(frame), seqid);

        data = residual;
        residual = null;
      }
    };
  }
}
