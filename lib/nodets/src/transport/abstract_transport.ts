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
import { OnFlushCallback, TransportState, ServiceClient } from '../thrift';
import {
  TApplicationExceptionType,
  TApplicationException,
  InputBufferUnderrunError
} from '../exception';

export default abstract class TAbstractTransport {
  public _seqid: number | null = null;
  public inBuf: Buffer = Buffer.alloc(0);
  public outBuffers: Buffer[] = [];
  public outCount: number = 0;
  public onFlush?: OnFlushCallback;
  public readCursor: number = 0;

  // Attached to transport from create_client.ts
  public client?: ServiceClient;

  public constructor(buffer?: Buffer, callback?: OnFlushCallback) {
    if (buffer) {
      this.inBuf = buffer;
    }
    this.onFlush = callback;
  }

  public reset(): void {
    this.inBuf = Buffer.alloc(0);
    this._seqid = null;
    this.outBuffers = [];
    this.outCount = 0;
    this.readCursor = 0;
  }

  public abstract commitPosition(): void;
  public abstract rollbackPosition(): void;
  public abstract flush(): void;
  public abstract borrow(): TransportState;

  // TODO: Implement open/close support
  public isOpen(): boolean {
    return true;
  }
  public open(): boolean {
    return true;
  }
  public close(): boolean {
    return true;
  }

  // Set the seqid of the message in the client
  // So that callbacks can be found
  public setCurrSeqId(seqid: number): void {
    this._seqid = seqid;
  }

  public ensureAvailable(len: number): void {
    if (this.readCursor + len > this.inBuf.length) {
      throw new InputBufferUnderrunError();
    }
  }

  public read(len: number): Buffer {
    // this function will be used for each frames.
    this.ensureAvailable(len);
    const end = this.readCursor + len;

    if (this.inBuf.length < end) {
      throw new Error('read(' + len + ') failed - not enough data');
    }

    const buf = this.inBuf.slice(this.readCursor, end);
    this.readCursor = end;
    return buf;
  }

  public readByte(): number {
    this.ensureAvailable(1);
    return binary.readByte(this.inBuf[this.readCursor++]);
  }

  public readI16(): number {
    this.ensureAvailable(2);
    const i16 = binary.readI16(this.inBuf, this.readCursor);
    this.readCursor += 2;
    return i16;
  }

  public readI32(): number {
    this.ensureAvailable(4);
    const i32 = binary.readI32(this.inBuf, this.readCursor);
    this.readCursor += 4;
    return i32;
  }

  public readDouble(): number {
    this.ensureAvailable(8);
    const d = binary.readDouble(this.inBuf, this.readCursor);
    this.readCursor += 8;
    return d;
  }

  public readString(len: number): string {
    this.ensureAvailable(len);
    const str = this.inBuf.toString(
      'utf8',
      this.readCursor,
      this.readCursor + len
    );
    this.readCursor += len;
    return str;
  }

  public consume(bytesConsumed: number): void {
    this.readCursor += bytesConsumed;
  }

  public write(buf: Buffer | string, encoding?: string): void {
    if (typeof buf === 'string') {
      buf = Buffer.from(buf, encoding || 'utf8');
    }
    this.outBuffers.push(buf);
    this.outCount += buf.length;
  }

  // Typescript doesn't support abstract static methods
  public static receiver(
    callback: Function,
    seqid: number
  ): (data: Buffer) => void {
    return () => {
      throw new TApplicationException(
        TApplicationExceptionType.INVALID_TRANSFORM,
        'Transports must implement receiver method'
      );
    };
  }
}
