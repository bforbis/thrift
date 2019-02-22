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
import * as binary from '../util/binary';
import Int64 from 'node-int64';
import * as Thrift from '../thrift';
import { TAbstractTransport } from '../transport';
import TAbstractProtocol from './abstract_protocol';

// JavaScript supports only numeric doubles, therefore even hex values are always signed.
// The largest integer value which can be represented in JavaScript is +/-2^53.
// Bitwise operations convert numbers to 32 bit integers but perform sign extension
// upon assigning values back to variables.
const VERSION_MASK = -65536, // 0xffff0000
  VERSION_1 = -2147418112, // 0x80010000
  TYPE_MASK = 0x000000ff;

export default class TBinaryProtocol extends TAbstractProtocol {
  public strictRead: boolean;
  public strictWrite: boolean;

  public constructor(
    trans: TAbstractTransport,
    strictRead?: boolean,
    strictWrite?: boolean
  ) {
    super(trans);
    this.strictRead = strictRead !== undefined ? strictRead : false;
    this.strictWrite = strictWrite !== undefined ? strictWrite : true;
  }

  public writeMessageBegin(
    name: string,
    type: Thrift.MessageType,
    seqid: number
  ): void {
    if (this.strictWrite) {
      this.writeI32(VERSION_1 | type);
      this.writeString(name);
      this.writeI32(seqid);
    } else {
      this.writeString(name);
      this.writeByte(type);
      this.writeI32(seqid);
    }
    // Record client seqid to find callback again
    if (this._seqid !== null) {
      log.warning('SeqId already set', { name: name });
    } else {
      this._seqid = seqid;
      this.trans.setCurrSeqId(seqid);
    }
  }

  public writeMessageEnd(): void {
    if (this._seqid !== null) {
      this._seqid = null;
    } else {
      log.warning('No seqid to unset');
    }
  }

  public writeStructBegin(name: string): void {}

  public writeStructEnd(): void {}

  public writeFieldBegin(name: string, type: Thrift.Type, id: number): void {
    this.writeByte(type);
    this.writeI16(id);
  }

  public writeFieldEnd(): void {}

  public writeFieldStop(): void {
    this.writeByte(Thrift.Type.STOP);
  }

  public writeMapBegin(
    ktype: Thrift.Type,
    vtype: Thrift.Type,
    size: number
  ): void {
    this.writeByte(ktype);
    this.writeByte(vtype);
    this.writeI32(size);
  }

  public writeMapEnd(): void {}

  public writeListBegin(etype: Thrift.Type, size: number): void {
    this.writeByte(etype);
    this.writeI32(size);
  }

  public writeListEnd(): void {}

  public writeSetBegin(etype: Thrift.Type, size: number): void {
    this.writeByte(etype);
    this.writeI32(size);
  }

  public writeSetEnd(): void {}

  public writeBool(bool: boolean): void {
    if (bool) {
      this.writeByte(1);
    } else {
      this.writeByte(0);
    }
  }

  public writeByte(b: number): void {
    this.trans.write(new Buffer([b]));
  }

  public writeI16(i16: number): void {
    this.trans.write(binary.writeI16(new Buffer(2), i16));
  }

  public writeI32(i32: number): void {
    this.trans.write(binary.writeI32(new Buffer(4), i32));
  }

  public writeI64(i64: number | Int64): void {
    if (i64 instanceof Int64) {
      this.trans.write(i64.buffer);
    } else {
      this.trans.write(new Int64(i64).buffer);
    }
  }

  public writeDouble(dub: number): void {
    this.trans.write(binary.writeDouble(new Buffer(8), dub));
  }

  private writeStringOrBinary(
    name: string,
    encoding: string,
    arg: string | Buffer | Uint8Array
  ): void {
    if (typeof arg === 'string') {
      this.writeI32(Buffer.byteLength(arg, encoding));
      this.trans.write(Buffer.from(arg, encoding));
    } else if (arg instanceof Buffer) {
      this.writeI32(arg.length);
      this.trans.write(arg);
    } else if (arg instanceof Uint8Array) {
      // Buffers in Node.js under Browserify may extend UInt8Array instead of
      // defining a new object. We detect them here so we can write them
      // correctly
      this.writeI32(arg.length);
      this.trans.write(Buffer.from(arg));
    } else {
      throw new Error(
        name + ' called without a string/Buffer argument: ' + arg
      );
    }
  }

  public writeString(arg: string | Buffer | Uint8Array): void {
    this.writeStringOrBinary('writeString', 'utf8', arg);
  }

  public writeBinary(arg: string | Buffer | Uint8Array): void {
    this.writeStringOrBinary('writeBinary', 'binary', arg);
  }

  public readMessageBegin(): Thrift.TMessage {
    const sz = this.readI32();
    let type: Thrift.MessageType, name: string, seqid: number;

    if (sz < 0) {
      const version = sz & VERSION_MASK;
      if (version != VERSION_1) {
        throw new Thrift.TProtocolException(
          Thrift.TProtocolExceptionType.BAD_VERSION,
          'Bad version in readMessageBegin: ' + sz
        );
      }
      type = sz & TYPE_MASK;
      name = this.readString();
      seqid = this.readI32();
    } else {
      if (this.strictRead) {
        throw new Thrift.TProtocolException(
          Thrift.TProtocolExceptionType.BAD_VERSION,
          'No protocol version header'
        );
      }
      name = this.trans.read(sz).toString();
      type = this.readByte();
      seqid = this.readI32();
    }
    return { fname: name, mtype: type, rseqid: seqid };
  }

  public readMessageEnd(): void {}

  public readStructBegin(): Thrift.TStruct {
    return { fname: '' };
  }

  public readStructEnd(): void {}

  public readFieldBegin(): Thrift.TField {
    const type = this.readByte();
    if (type == Thrift.Type.STOP) {
      return { fname: '', ftype: type, fid: 0 };
    }
    const id = this.readI16();
    return { fname: '', ftype: type, fid: id };
  }

  public readFieldEnd(): void {}

  public readMapBegin(): Thrift.TMap {
    const ktype = this.readByte();
    const vtype = this.readByte();
    const size = this.readI32();
    return { ktype: ktype, vtype: vtype, size: size };
  }

  public readMapEnd(): void {}

  public readListBegin(): Thrift.TList {
    const etype = this.readByte();
    const size = this.readI32();
    return { etype: etype, size: size };
  }

  public readListEnd(): void {}

  public readSetBegin(): Thrift.TSet {
    const etype = this.readByte();
    const size = this.readI32();
    return { etype: etype, size: size };
  }

  public readSetEnd(): void {}

  public readBool(): boolean {
    const b = this.readByte();
    if (b === 0) {
      return false;
    }
    return true;
  }

  public readByte(): number {
    return this.trans.readByte();
  }

  public readI16(): number {
    return this.trans.readI16();
  }

  public readI32(): number {
    return this.trans.readI32();
  }

  public readI64(): Int64 {
    const buff = this.trans.read(8);
    return new Int64(buff);
  }

  public readDouble(): number {
    return this.trans.readDouble();
  }

  public readBinary(): Buffer {
    const len = this.readI32();
    if (len === 0) {
      return new Buffer(0);
    }

    if (len < 0) {
      throw new Thrift.TProtocolException(
        Thrift.TProtocolExceptionType.NEGATIVE_SIZE,
        'Negative binary size'
      );
    }
    return this.trans.read(len);
  }

  public readString(): string {
    const len = this.readI32();
    if (len === 0) {
      return '';
    }

    if (len < 0) {
      throw new Thrift.TProtocolException(
        Thrift.TProtocolExceptionType.NEGATIVE_SIZE,
        'Negative string size'
      );
    }
    return this.trans.readString(len);
  }
}
