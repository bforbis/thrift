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
import Int64 from 'node-int64';
import * as Thrift from '../thrift';
import { TAbstractTransport } from '../transport';
import TAbstractProtocol from './abstract_protocol';

const POW_8 = Math.pow(2, 8);
const POW_24 = Math.pow(2, 24);
const POW_32 = Math.pow(2, 32);
const POW_40 = Math.pow(2, 40);
const POW_48 = Math.pow(2, 48);
const POW_52 = Math.pow(2, 52);
const POW_1022 = Math.pow(2, 1022);

// Compact Protocol Constants
//
/**
 * Compact Protocol ID number.
 * @readonly
 * @const {number} PROTOCOL_ID
 */
const PROTOCOL_ID = -126; //1000 0010

/**
 * Compact Protocol version number.
 * @readonly
 * @const {number} VERSION_N
 */
const VERSION_N = 1;

/**
 * Compact Protocol version mask for combining protocol version and message type in one byte.
 * @readonly
 * @const {number} VERSION_MASK
 */
const VERSION_MASK = 0x1f; //0001 1111

/**
 * Compact Protocol message type mask for combining protocol version and message type in one byte.
 * @readonly
 * @const {number} TYPE_MASK
 */
const TYPE_MASK = -32; //1110 0000

/**
 * Compact Protocol message type bits for ensuring message type bit size.
 * @readonly
 * @const {number} TYPE_BITS
 */
const TYPE_BITS = 7; //0000 0111

/**
 * Compact Protocol message type shift amount for combining protocol version and message type in one byte.
 * @readonly
 * @const {number} TYPE_SHIFT_AMOUNT
 */
const TYPE_SHIFT_AMOUNT = 5;

/**
 * Compact Protocol type IDs used to keep type data within one nibble.
 * @readonly
 * @property {number}  CT_STOP          - End of a set of fields.
 * @property {number}  CT_BOOLEAN_TRUE  - Flag for Boolean field with true value (packed field and value).
 * @property {number}  CT_BOOLEAN_FALSE - Flag for Boolean field with false value (packed field and value).
 * @property {number}  CT_BYTE          - Signed 8 bit integer.
 * @property {number}  CT_I16           - Signed 16 bit integer.
 * @property {number}  CT_I32           - Signed 32 bit integer.
 * @property {number}  CT_I64           - Signed 64 bit integer (2^53 max in JavaScript).
 * @property {number}  CT_DOUBLE        - 64 bit IEEE 854 floating point.
 * @property {number}  CT_BINARY        - Array of bytes (used for strings also).
 * @property {number}  CT_LIST          - A collection type (unordered).
 * @property {number}  CT_SET           - A collection type (unordered and without repeated values).
 * @property {number}  CT_MAP           - A collection type (map/associative-array/dictionary).
 * @property {number}  CT_STRUCT        - A multifield type.
 */
enum CTypes {
  CT_STOP = 0x00,
  CT_BOOLEAN_TRUE = 0x01,
  CT_BOOLEAN_FALSE = 0x02,
  CT_BYTE = 0x03,
  CT_I16 = 0x04,
  CT_I32 = 0x05,
  CT_I64 = 0x06,
  CT_DOUBLE = 0x07,
  CT_BINARY = 0x08,
  CT_LIST = 0x09,
  CT_SET = 0x0a,
  CT_MAP = 0x0b,
  CT_STRUCT = 0x0c
}

/**
 * Array mapping Compact type IDs to standard Thrift type IDs.
 * @readonly
 */
const TTypeToCType = [
  CTypes.CT_STOP, // T_STOP
  0, // unused
  CTypes.CT_BOOLEAN_TRUE, // T_BOOL
  CTypes.CT_BYTE, // T_BYTE
  CTypes.CT_DOUBLE, // T_DOUBLE
  0, // unused
  CTypes.CT_I16, // T_I16
  0, // unused
  CTypes.CT_I32, // T_I32
  0, // unused
  CTypes.CT_I64, // T_I64
  CTypes.CT_BINARY, // T_STRING
  CTypes.CT_STRUCT, // T_STRUCT
  CTypes.CT_MAP, // T_MAP
  CTypes.CT_SET, // T_SET
  CTypes.CT_LIST // T_LIST
];

interface BooleanField {
  name: string | null;
  hasBoolValue: boolean;
  fieldType: Thrift.Type | null;
  fieldId: number | null;
}

interface BoolValue {
  hasBoolValue: boolean;
  boolValue: boolean;
}

export default class TCompactProtocol extends TAbstractProtocol {
  private lastField_: number[] = [];
  private lastFieldId_: number = 0;
  private booleanField_: BooleanField = {
    name: null,
    hasBoolValue: false,
    fieldType: null,
    fieldId: null
  };
  private boolValue_: BoolValue = {
    hasBoolValue: false,
    boolValue: false
  };

  /**
   * Constructor Function for the Compact Protocol.
   * @constructor
   * @param {object} [trans] - The underlying transport to read/write.
   * @classdesc The Apache Thrift Protocol layer performs serialization
   *     of base types, the compact protocol serializes data in binary
   *     form with minimal space used for scalar values.
   */
  public constructor(trans: TAbstractTransport) {
    super(trans);
  }

  /**
   * Lookup a Compact Protocol Type value for a given Thrift Type value.
   * N.B. Used only internally.
   * @param {number} ttype - Thrift type value
   * @returns {number} Compact protocol type value
   */
  public getCompactType(ttype: Thrift.Type): CTypes {
    return TTypeToCType[ttype];
  }

  /**
   * Lookup a Thrift Type value for a given Compact Protocol Type value.
   * N.B. Used only internally.
   * @param {number} type - Compact Protocol type value
   * @returns {number} Thrift Type value
   */
  public getTType(type: CTypes | number): Thrift.Type {
    switch (type) {
      case Thrift.Type.STOP:
        return Thrift.Type.STOP;
      case CTypes.CT_BOOLEAN_FALSE:
      case CTypes.CT_BOOLEAN_TRUE:
        return Thrift.Type.BOOL;
      case CTypes.CT_BYTE:
        return Thrift.Type.BYTE;
      case CTypes.CT_I16:
        return Thrift.Type.I16;
      case CTypes.CT_I32:
        return Thrift.Type.I32;
      case CTypes.CT_I64:
        return Thrift.Type.I64;
      case CTypes.CT_DOUBLE:
        return Thrift.Type.DOUBLE;
      case CTypes.CT_BINARY:
        return Thrift.Type.STRING;
      case CTypes.CT_LIST:
        return Thrift.Type.LIST;
      case CTypes.CT_SET:
        return Thrift.Type.SET;
      case CTypes.CT_MAP:
        return Thrift.Type.MAP;
      case CTypes.CT_STRUCT:
        return Thrift.Type.STRUCT;
      default:
        throw new Thrift.TProtocolException(
          Thrift.TProtocolExceptionType.INVALID_DATA,
          'Unknown type: ' + type
        );
    }
  }

  //
  // Compact Protocol write operations
  //

  /**
   * Writes an RPC message header
   * @param {string} name - The method name for the message.
   * @param {number} type - The type of message (CALL, REPLY, EXCEPTION, ONEWAY).
   * @param {number} seqid - The call sequence number (if any).
   */
  public writeMessageBegin(
    name: string,
    type: Thrift.MessageType,
    seqid: number
  ): void {
    this.writeByte(PROTOCOL_ID);
    this.writeByte(
      (VERSION_N & VERSION_MASK) | ((type << TYPE_SHIFT_AMOUNT) & TYPE_MASK)
    );
    this.writeVarint32(seqid);
    this.writeString(name);

    // Record client seqid to find callback again
    if (this._seqid !== null) {
      log.warning('SeqId already set', { name: name });
    } else {
      this._seqid = seqid;
      this.trans.setCurrSeqId(seqid);
    }
  }

  public writeMessageEnd(): void {}

  public writeStructBegin(name: string): void {
    this.lastField_.push(this.lastFieldId_);
    this.lastFieldId_ = 0;
  }

  public writeStructEnd(): void {
    this.lastFieldId_ = this.lastField_.pop() || 0;
  }

  /**
   * Writes a struct field header
   * @param {string} name - The field name (not written with the compact protocol).
   * @param {number} type - The field data type (a normal Thrift field Type).
   * @param {number} id - The IDL field Id.
   */
  public writeFieldBegin(name: string, type: Thrift.Type, id: number): void {
    if (type != Thrift.Type.BOOL) {
      return this.writeFieldBeginInternal(name, type, id, -1);
    }

    this.booleanField_.name = name;
    this.booleanField_.fieldType = type;
    this.booleanField_.fieldId = id;
  }

  public writeFieldEnd(): void {}

  public writeFieldStop(): void {
    this.writeByte(CTypes.CT_STOP);
  }

  /**
   * Writes a map collection header
   * @param {number} keyType - The Thrift type of the map keys.
   * @param {number} valType - The Thrift type of the map values.
   * @param {number} size - The number of k/v pairs in the map.
   */
  public writeMapBegin(
    keyType: Thrift.Type,
    valType: Thrift.Type,
    size: number
  ): void {
    if (size === 0) {
      this.writeByte(0);
    } else {
      this.writeVarint32(size);
      this.writeByte(
        (this.getCompactType(keyType) << 4) | this.getCompactType(valType)
      );
    }
  }

  public writeMapEnd(): void {}

  /**
   * Writes a list collection header
   * @param {number} elemType - The Thrift type of the list elements.
   * @param {number} size - The number of elements in the list.
   */
  public writeListBegin(elemType: number, size: number): void {
    this.writeCollectionBegin(elemType, size);
  }

  public writeListEnd(): void {}

  /**
   * Writes a set collection header
   * @param {number} elemType - The Thrift type of the set elements.
   * @param {number} size - The number of elements in the set.
   */
  public writeSetBegin(elemType: number, size: number): void {
    this.writeCollectionBegin(elemType, size);
  }

  public writeSetEnd(): void {}

  public writeBool(value: boolean): void {
    if (
      this.booleanField_.name !== null &&
      this.booleanField_.fieldType !== null &&
      this.booleanField_.fieldId !== null
    ) {
      // we haven't written the field header yet
      this.writeFieldBeginInternal(
        this.booleanField_.name,
        this.booleanField_.fieldType,
        this.booleanField_.fieldId,
        value ? CTypes.CT_BOOLEAN_TRUE : CTypes.CT_BOOLEAN_FALSE
      );
      this.booleanField_.name = null;
    } else {
      // we're not part of a field, so just write the value
      this.writeByte(value ? CTypes.CT_BOOLEAN_TRUE : CTypes.CT_BOOLEAN_FALSE);
    }
  }

  public writeByte(b: number): void {
    this.trans.write(new Buffer([b]));
  }

  public writeI16(i16: number): void {
    this.writeVarint32(this.i32ToZigzag(i16));
  }

  public writeI32(i32: number): void {
    this.writeVarint32(this.i32ToZigzag(i32));
  }

  public writeI64(i64: number | Int64): void {
    this.writeVarint64(this.i64ToZigzag(i64));
  }

  // Little-endian, unlike TBinaryProtocol
  public writeDouble(v: number): void {
    const buff = new Buffer(8);
    let m, e, c;

    buff[7] = v < 0 ? 0x80 : 0x00;

    v = Math.abs(v);
    if (v !== v) {
      // NaN, use QNaN IEEE format
      m = 2251799813685248;
      e = 2047;
    } else if (v === Infinity) {
      m = 0;
      e = 2047;
    } else {
      e = Math.floor(Math.log(v) / Math.LN2);
      c = Math.pow(2, -e);
      if (v * c < 1) {
        e--;
        c *= 2;
      }

      if (e + 1023 >= 2047) {
        // Overflow
        m = 0;
        e = 2047;
      } else if (e + 1023 >= 1) {
        // Normalized - term order matters, as Math.pow(2, 52-e) and v*Math.pow(2, 52) can overflow
        m = (v * c - 1) * POW_52;
        e += 1023;
      } else {
        // Denormalized - also catches the '0' case, somewhat by chance
        m = v * POW_1022 * POW_52;
        e = 0;
      }
    }

    buff[6] = (e << 4) & 0xf0;
    buff[7] |= (e >> 4) & 0x7f;

    buff[0] = m & 0xff;
    m = Math.floor(m / POW_8);
    buff[1] = m & 0xff;
    m = Math.floor(m / POW_8);
    buff[2] = m & 0xff;
    m = Math.floor(m / POW_8);
    buff[3] = m & 0xff;
    m >>= 8;
    buff[4] = m & 0xff;
    m >>= 8;
    buff[5] = m & 0xff;
    m >>= 8;
    buff[6] |= m & 0x0f;

    this.trans.write(buff);
  }

  public writeStringOrBinary(
    name: string,
    encoding: string,
    arg: string | Buffer | Uint8Array
  ): void {
    if (typeof arg === 'string') {
      this.writeVarint32(Buffer.byteLength(arg, encoding));
      this.trans.write(Buffer.from(arg, encoding));
    } else if (arg instanceof Buffer) {
      this.writeVarint32(arg.length);
      this.trans.write(arg);
    } else if (arg instanceof Uint8Array) {
      // Buffers in Node.js under Browserify may extend UInt8Array instead of
      // defining a new object. We detect them here so we can write them
      // correctly
      this.writeVarint32(arg.length);
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

  //
  // Compact Protocol internal write methods
  //

  private writeFieldBeginInternal(
    name: string,
    fieldType: Thrift.Type,
    fieldId: number,
    typeOverride: CTypes
  ): void {
    //If there's a type override, use that.
    const typeToWrite =
      typeOverride == -1 ? this.getCompactType(fieldType) : typeOverride;
    //Check if we can delta encode the field id
    if (fieldId > this.lastFieldId_ && fieldId - this.lastFieldId_ <= 15) {
      //Include the type delta with the field ID
      this.writeByte(((fieldId - this.lastFieldId_) << 4) | typeToWrite);
    } else {
      //Write separate type and ID values
      this.writeByte(typeToWrite);
      this.writeI16(fieldId);
    }
    this.lastFieldId_ = fieldId;
  }

  private writeCollectionBegin(elemType: Thrift.Type, size: number): void {
    if (size <= 14) {
      //Combine size and type in one byte if possible
      this.writeByte((size << 4) | this.getCompactType(elemType));
    } else {
      this.writeByte(0xf0 | this.getCompactType(elemType));
      this.writeVarint32(size);
    }
  }

  /**
   * Write an i32 as a varint. Results in 1-5 bytes on the wire.
   */
  private writeVarint32(n: number): void {
    const buf = new Buffer(5);
    let wsize = 0;
    while (true) {
      if ((n & ~0x7f) === 0) {
        buf[wsize++] = n;
        break;
      } else {
        buf[wsize++] = (n & 0x7f) | 0x80;
        n = n >>> 7;
      }
    }
    const wbuf = new Buffer(wsize);
    buf.copy(wbuf, 0, 0, wsize);
    this.trans.write(wbuf);
  }

  /**
   * Write an i64 as a varint. Results in 1-10 bytes on the wire.
   * N.B. node-int64 is always big endian
   */
  private writeVarint64(n: number | Int64): void {
    if (typeof n === 'number') {
      n = new Int64(n);
    }
    if (!(n instanceof Int64)) {
      throw new Thrift.TProtocolException(
        Thrift.TProtocolExceptionType.INVALID_DATA,
        'Expected Int64 or Number, found: ' + n
      );
    }

    const buf = new Buffer(10);
    let wsize = 0;
    let hi = n.buffer.readUInt32BE(0, true);
    let lo = n.buffer.readUInt32BE(4, true);
    let mask = 0;
    while (true) {
      if ((lo & ~0x7f) === 0 && hi === 0) {
        buf[wsize++] = lo;
        break;
      } else {
        buf[wsize++] = (lo & 0x7f) | 0x80;
        mask = hi << 25;
        lo = lo >>> 7;
        hi = hi >>> 7;
        lo = lo | mask;
      }
    }
    const wbuf = new Buffer(wsize);
    buf.copy(wbuf, 0, 0, wsize);
    this.trans.write(wbuf);
  }

  /**
   * Convert l into a zigzag long. This allows negative numbers to be
   * represented compactly as a varint.
   */
  private i64ToZigzag(l: string | number | Int64): Int64 {
    if (typeof l === 'string') {
      l = new Int64(parseInt(l, 10));
    } else if (typeof l === 'number') {
      l = new Int64(l);
    }
    if (!(l instanceof Int64)) {
      throw new Thrift.TProtocolException(
        Thrift.TProtocolExceptionType.INVALID_DATA,
        'Expected Int64 or Number, found: ' + l
      );
    }
    let hi = l.buffer.readUInt32BE(0, true);
    let lo = l.buffer.readUInt32BE(4, true);
    const sign = hi >>> 31;
    hi = ((hi << 1) | (lo >>> 31)) ^ (sign ? 0xffffffff : 0);
    lo = (lo << 1) ^ (sign ? 0xffffffff : 0);
    return new Int64(hi, lo);
  }

  /**
   * Convert n into a zigzag int. This allows negative numbers to be
   * represented compactly as a varint.
   */
  private i32ToZigzag(n: number): number {
    return (n << 1) ^ (n & 0x80000000 ? 0xffffffff : 0);
  }

  //
  // Compact Protocol read operations
  //

  public readMessageBegin(): Thrift.TMessage {
    //Read protocol ID
    const protocolId = this.trans.readByte();
    if (protocolId != PROTOCOL_ID) {
      throw new Thrift.TProtocolException(
        Thrift.TProtocolExceptionType.BAD_VERSION,
        'Bad protocol identifier ' + protocolId
      );
    }

    //Read Version and Type
    const versionAndType = this.trans.readByte();
    const version = versionAndType & VERSION_MASK;
    if (version != VERSION_N) {
      throw new Thrift.TProtocolException(
        Thrift.TProtocolExceptionType.BAD_VERSION,
        'Bad protocol version ' + version
      );
    }
    const type = (versionAndType >> TYPE_SHIFT_AMOUNT) & TYPE_BITS;

    //Read SeqId
    const seqid = this.readVarint32();

    //Read name
    const name = this.readString();

    return { fname: name, mtype: type, rseqid: seqid };
  }

  public readMessageEnd(): void {}

  public readStructBegin(): Thrift.TStruct {
    this.lastField_.push(this.lastFieldId_);
    this.lastFieldId_ = 0;
    return { fname: '' };
  }

  public readStructEnd(): void {
    this.lastFieldId_ = this.lastField_.pop() || 0;
  }

  public readFieldBegin(): Thrift.TField {
    let fieldId = 0;
    const b = this.trans.readByte();
    const type = b & 0x0f;

    if (type == CTypes.CT_STOP) {
      return { fname: '', ftype: Thrift.Type.STOP, fid: 0 };
    }

    //Mask off the 4 MSB of the type header to check for field id delta.
    const modifier = (b & 0x000000f0) >>> 4;
    if (modifier === 0) {
      //If not a delta read the field id.
      fieldId = this.readI16();
    } else {
      //Recover the field id from the delta
      fieldId = this.lastFieldId_ + modifier;
    }
    const fieldType = this.getTType(type);

    //Boolean are encoded with the type
    if (type == CTypes.CT_BOOLEAN_TRUE || type == CTypes.CT_BOOLEAN_FALSE) {
      this.boolValue_.hasBoolValue = true;
      this.boolValue_.boolValue = type == CTypes.CT_BOOLEAN_TRUE ? true : false;
    }

    //Save the new field for the next delta computation.
    this.lastFieldId_ = fieldId;
    return { fname: '', ftype: fieldType, fid: fieldId };
  }

  public readFieldEnd(): void {}

  public readMapBegin(): Thrift.TMap {
    const msize = this.readVarint32();
    if (msize < 0) {
      throw new Thrift.TProtocolException(
        Thrift.TProtocolExceptionType.NEGATIVE_SIZE,
        'Negative map size'
      );
    }

    let kvType = 0;
    if (msize !== 0) {
      kvType = this.trans.readByte();
    }

    const keyType = this.getTType((kvType & 0xf0) >>> 4);
    const valType = this.getTType(kvType & 0xf);
    return { ktype: keyType, vtype: valType, size: msize };
  }

  public readMapEnd(): void {}

  public readListBegin(): Thrift.TList {
    const size_and_type = this.trans.readByte();

    let lsize = (size_and_type >>> 4) & 0x0000000f;
    if (lsize == 15) {
      lsize = this.readVarint32();
    }

    if (lsize < 0) {
      throw new Thrift.TProtocolException(
        Thrift.TProtocolExceptionType.NEGATIVE_SIZE,
        'Negative list size'
      );
    }

    const elemType = this.getTType(size_and_type & 0x0000000f);

    return { etype: elemType, size: lsize };
  }

  public readListEnd(): void {}

  public readSetBegin(): Thrift.TList {
    return this.readListBegin();
  }

  public readSetEnd(): void {}

  public readBool(): boolean {
    let value = false;
    if (this.boolValue_.hasBoolValue === true) {
      value = this.boolValue_.boolValue;
      this.boolValue_.hasBoolValue = false;
    } else {
      return this.trans.readByte() == CTypes.CT_BOOLEAN_TRUE;
    }
    return value;
  }

  public readByte(): number {
    return this.trans.readByte();
  }

  public readI16(): number {
    return this.readI32();
  }

  public readI32(): number {
    return this.zigzagToI32(this.readVarint32());
  }

  public readI64(): Int64 {
    return this.zigzagToI64(this.readVarint64());
  }

  // Little-endian, unlike TBinaryProtocol
  public readDouble(): number {
    const buff = this.trans.read(8);
    const off = 0;

    const signed = buff[off + 7] & 0x80;
    let e = (buff[off + 6] & 0xf0) >> 4;
    e += (buff[off + 7] & 0x7f) << 4;

    let m = buff[off];
    m += buff[off + 1] << 8;
    m += buff[off + 2] << 16;
    m += buff[off + 3] * POW_24;
    m += buff[off + 4] * POW_32;
    m += buff[off + 5] * POW_40;
    m += (buff[off + 6] & 0x0f) * POW_48;

    switch (e) {
      case 0:
        e = -1022;
        break;
      case 2047:
        return m ? NaN : signed ? -Infinity : Infinity;
      default:
        m += POW_52;
        e -= 1023;
    }

    if (signed) {
      m *= -1;
    }

    return m * Math.pow(2, e - 52);
  }

  public readBinary(): Buffer {
    const size = this.readVarint32();
    if (size === 0) {
      return Buffer.alloc(0);
    }

    if (size < 0) {
      throw new Thrift.TProtocolException(
        Thrift.TProtocolExceptionType.NEGATIVE_SIZE,
        'Negative binary size'
      );
    }
    return this.trans.read(size);
  }

  public readString(): string {
    const size = this.readVarint32();
    // Catch empty string case
    if (size === 0) {
      return '';
    }

    // Catch error cases
    if (size < 0) {
      throw new Thrift.TProtocolException(
        Thrift.TProtocolExceptionType.NEGATIVE_SIZE,
        'Negative string size'
      );
    }
    return this.trans.readString(size);
  }

  //
  // Compact Protocol internal read operations
  //

  /**
   * Read an i32 from the wire as a varint. The MSB of each byte is set
   * if there is another byte to follow. This can read up to 5 bytes.
   */
  private readVarint32(): number {
    return this.readVarint64().toNumber();
  }

  /**
   * Read an i64 from the wire as a proper varint. The MSB of each byte is set
   * if there is another byte to follow. This can read up to 10 bytes.
   */
  private readVarint64(): Int64 {
    let rsize = 0;
    let lo = 0;
    let hi = 0;
    let shift = 0;
    while (true) {
      const b = this.trans.readByte();
      rsize++;
      if (shift <= 25) {
        lo = lo | ((b & 0x7f) << shift);
      } else if (25 < shift && shift < 32) {
        lo = lo | ((b & 0x7f) << shift);
        hi = hi | ((b & 0x7f) >>> (32 - shift));
      } else {
        hi = hi | ((b & 0x7f) << (shift - 32));
      }
      shift += 7;
      if (!(b & 0x80)) {
        break;
      }
      if (rsize >= 10) {
        throw new Thrift.TProtocolException(
          Thrift.TProtocolExceptionType.INVALID_DATA,
          'Variable-length int over 10 bytes.'
        );
      }
    }
    return new Int64(hi, lo);
  }

  /**
   * Convert from zigzag int to int.
   */
  private zigzagToI32(n: number): number {
    return (n >>> 1) ^ (-1 * (n & 1));
  }

  /**
   * Convert from zigzag long to long.
   */
  private zigzagToI64(n: Int64): Int64 {
    let hi = n.buffer.readUInt32BE(0, true);
    let lo = n.buffer.readUInt32BE(4, true);

    const neg = new Int64(hi & 0, lo & 1);
    neg._2scomp();
    const hi_neg = neg.buffer.readUInt32BE(0, true);
    const lo_neg = neg.buffer.readUInt32BE(4, true);

    const hi_lo = hi << 31;
    hi = (hi >>> 1) ^ hi_neg;
    lo = ((lo >>> 1) | hi_lo) ^ lo_neg;
    return new Int64(hi, lo);
  }
}
