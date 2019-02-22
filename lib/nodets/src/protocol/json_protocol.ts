/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import Int64 from 'node-int64';
import { isNumber, isArray } from 'util';

import * as Thrift from '../thrift';
import * as Int64Util from '../util/int64_util';
import json_parse from './json_parse';
import { InputBufferUnderrunError } from '../exception';
import { TAbstractTransport } from '../transport';
import TAbstractProtocol from './abstract_protocol';

/**
 * The TJSONProtocol version number.
 * @readonly
 * @const {number} Version
 * @memberof Thrift.Protocol
 */
const VERSION = 1;

/**
 * Thrift IDL type Id to string mapping.
 * @readonly
 * @see {@link Thrift.Type}
 */
const JSONProtocolType: { [key: number]: string } = {};
JSONProtocolType[Thrift.Type.BOOL] = '"tf"';
JSONProtocolType[Thrift.Type.BYTE] = '"i8"';
JSONProtocolType[Thrift.Type.I16] = '"i16"';
JSONProtocolType[Thrift.Type.I32] = '"i32"';
JSONProtocolType[Thrift.Type.I64] = '"i64"';
JSONProtocolType[Thrift.Type.DOUBLE] = '"dbl"';
JSONProtocolType[Thrift.Type.STRUCT] = '"rec"';
JSONProtocolType[Thrift.Type.STRING] = '"str"';
JSONProtocolType[Thrift.Type.MAP] = '"map"';
JSONProtocolType[Thrift.Type.LIST] = '"lst"';
JSONProtocolType[Thrift.Type.SET] = '"set"';

/**
 * Thrift IDL type string to Id mapping.
 * @readonly
 * @see {@link Thrift.Type}
 */
const RType: { [key: string]: Thrift.Type } = {
  tf: Thrift.Type.BOOL,
  i8: Thrift.Type.BYTE,
  i16: Thrift.Type.I16,
  i32: Thrift.Type.I32,
  i64: Thrift.Type.I64,
  dbl: Thrift.Type.DOUBLE,
  rec: Thrift.Type.STRUCT,
  str: Thrift.Type.STRING,
  map: Thrift.Type.MAP,
  lst: Thrift.Type.LIST,
  set: Thrift.Type.SET
};

export default class TJSONProtocol extends TAbstractProtocol {
  private tstack: any[] = [];
  private tpos: number[] = [];
  private rstack: any[] = [];
  private rpos: number[] = [];

  /**
   * Initializes a Thrift JSON protocol instance.
   * @constructor
   * @param {Thrift.Transport} trans - The transport to serialize to/from.
   * @classdesc Apache Thrift Protocols perform serialization which enables cross
   * language RPC. The Protocol type is the JavaScript browser implementation
   * of the Apache Thrift TJSONProtocol.
   * @example
   *     var protocol  = new Thrift.Protocol(transport);
   */
  public constructor(trans: TAbstractTransport) {
    super(trans);
  }

  public flush(): void {
    this.writeToTransportIfStackIsFlushable();
    return super.flush();
  }

  private writeToTransportIfStackIsFlushable(): void {
    if (this.tstack.length === 1) {
      this.trans.write(this.tstack.pop());
    }
  }

  /**
   * Serializes the beginning of a Thrift RPC message.
   * @param {string} name - The service method to call.
   * @param {Thrift.MessageType} messageType - The type of method call.
   * @param {number} seqid - The sequence number of this call (always 0 in Apache Thrift).
   */
  public writeMessageBegin(
    name: string,
    messageType: Thrift.MessageType,
    seqid: number
  ): void {
    this.tstack.push([VERSION, `"${name}"`, messageType, seqid]);
  }

  /**
   * Serializes the end of a Thrift RPC message.
   */
  public writeMessageEnd(): void {
    const obj = this.tstack.pop();

    const wobj = this.tstack.pop();
    wobj.push(obj);

    const wbuf = `[${wobj.join(',')}]`;

    // we assume there is nothing more to come so we write
    this.trans.write(wbuf);
  }

  /**
   * Serializes the beginning of a struct.
   * @param {string} name - The name of the struct.
   */
  public writeStructBegin(name: string): void {
    this.tpos.push(this.tstack.length);
    this.tstack.push({});
  }

  /**
   * Serializes the end of a struct.
   */
  public writeStructEnd(): void {
    const p = safePopPosition(this.tpos);

    const struct = this.tstack[p];
    let str = '{';
    let first = true;
    for (const key in struct) {
      if (first) {
        first = false;
      } else {
        str += ',';
      }

      str += key + ':' + struct[key];
    }

    str += '}';
    this.tstack[p] = str;

    this.writeToTransportIfStackIsFlushable();
  }

  /**
   * Serializes the beginning of a struct field.
   * @param {string} name - The name of the field.
   * @param {Thrift.Protocol.Type} fieldType - The data type of the field.
   * @param {number} fieldId - The field's unique identifier.
   */
  public writeFieldBegin(
    name: string,
    fieldType: Thrift.Type,
    fieldId: number
  ): void {
    this.tpos.push(this.tstack.length);
    this.tstack.push({
      fieldId: '"' + fieldId + '"',
      fieldType: JSONProtocolType[fieldType]
    });
  }

  /**
   * Serializes the end of a field.
   */
  public writeFieldEnd(): void {
    const value = this.tstack.pop();
    const fieldInfo = this.tstack.pop();

    if (':' + value === ':[object Object]') {
      this.tstack[this.tstack.length - 1][fieldInfo.fieldId] =
        '{' + fieldInfo.fieldType + ':' + JSON.stringify(value) + '}';
    } else {
      this.tstack[this.tstack.length - 1][fieldInfo.fieldId] =
        '{' + fieldInfo.fieldType + ':' + value + '}';
    }
    this.tpos.pop();

    this.writeToTransportIfStackIsFlushable();
  }

  /**
   * Serializes the end of the set of fields for a struct.
   */
  public writeFieldStop(): void {}

  /**
   * Serializes the beginning of a map collection.
   * @param {Thrift.Type} keyType - The data type of the key.
   * @param {Thrift.Type} valType - The data type of the value.
   * @param {number} [size] - The number of elements in the map (ignored).
   */
  public writeMapBegin(
    keyType: Thrift.Type,
    valType: Thrift.Type,
    size: number
  ): void {
    //size is invalid, we'll set it on end.
    this.tpos.push(this.tstack.length);
    this.tstack.push([JSONProtocolType[keyType], JSONProtocolType[valType], 0]);
  }

  /**
   * Serializes the end of a map.
   */
  public writeMapEnd(): void {
    const p = safePopPosition(this.tpos);

    if (p == this.tstack.length) {
      return;
    }
    if ((this.tstack.length - p - 1) % 2 !== 0) {
      this.tstack.push('');
    }

    const size = (this.tstack.length - p - 1) / 2;

    this.tstack[p][this.tstack[p].length - 1] = size;

    let map = '}';
    let first = true;
    while (this.tstack.length > p + 1) {
      const v = this.tstack.pop();
      let k = this.tstack.pop();
      if (first) {
        first = false;
      } else {
        map = ',' + map;
      }

      if (!isNaN(k)) {
        k = '"' + k + '"';
      } //json "keys" need to be strings
      map = k + ':' + v + map;
    }
    map = '{' + map;

    this.tstack[p].push(map);
    this.tstack[p] = '[' + this.tstack[p].join(',') + ']';

    this.writeToTransportIfStackIsFlushable();
  }

  /**
   * Serializes the beginning of a list collection.
   * @param {Thrift.Type} elemType - The data type of the elements.
   * @param {number} size - The number of elements in the list.
   */
  public writeListBegin(elemType: Thrift.Type, size: number): void {
    this.tpos.push(this.tstack.length);
    this.tstack.push([JSONProtocolType[elemType], size]);
  }

  /**
   * Serializes the end of a list.
   */
  public writeListEnd(): void {
    const p = safePopPosition(this.tpos);

    while (this.tstack.length > p + 1) {
      const tmpVal = this.tstack[p + 1];
      this.tstack.splice(p + 1, 1);
      this.tstack[p].push(tmpVal);
    }

    this.tstack[p] = '[' + this.tstack[p].join(',') + ']';

    this.writeToTransportIfStackIsFlushable();
  }

  /**
   * Serializes the beginning of a set collection.
   * @param {Thrift.Type} elemType - The data type of the elements.
   * @param {number} size - The number of elements in the list.
   */
  public writeSetBegin(elemType: Thrift.Type, size: number): void {
    this.tpos.push(this.tstack.length);
    this.tstack.push([JSONProtocolType[elemType], size]);
  }

  /**
   * Serializes the end of a set.
   */
  public writeSetEnd(): void {
    const p = safePopPosition(this.tpos);

    while (this.tstack.length > p + 1) {
      const tmpVal = this.tstack[p + 1];
      this.tstack.splice(p + 1, 1);
      this.tstack[p].push(tmpVal);
    }

    this.tstack[p] = '[' + this.tstack[p].join(',') + ']';

    this.writeToTransportIfStackIsFlushable();
  }

  /** Serializes a boolean */
  public writeBool(bool: boolean): void {
    this.tstack.push(bool ? 1 : 0);
  }

  /** Serializes a number */
  public writeByte(byte: number): void {
    this.tstack.push(byte);
  }

  /** Serializes a number */
  public writeI16(i16: number): void {
    this.tstack.push(i16);
  }

  /** Serializes a number */
  public writeI32(i32: number): void {
    this.tstack.push(i32);
  }

  /** Serializes a number */
  public writeI64(i64: Int64 | number): void {
    if (i64 instanceof Int64) {
      this.tstack.push(Int64Util.toDecimalString(i64));
    } else {
      this.tstack.push(i64);
    }
  }

  /** Serializes a number */
  public writeDouble(dub: number): void {
    this.tstack.push(dub);
  }

  /** Serializes a string */
  public writeString(arg: string | Buffer): void {
    // We do not encode uri components for wire transfer:
    if (arg === null) {
      this.tstack.push(null);
    } else {
      let str: string;
      if (typeof arg === 'string') {
        str = arg;
      } else if (arg instanceof Buffer) {
        str = arg.toString('utf8');
      } else {
        throw new Error(
          'writeString called without a string/Buffer argument: ' + arg
        );
      }

      // concat may be slower than building a byte buffer
      let escapedString = '';
      for (let i = 0; i < str.length; i++) {
        const ch = str.charAt(i); // a single double quote: "
        if (ch === '"') {
          escapedString += '\\"'; // write out as: \"
        } else if (ch === '\\') {
          // a single backslash: \
          escapedString += '\\\\'; // write out as: \\
          /* Currently escaped forward slashes break TJSONProtocol.
           * As it stands, we can simply pass forward slashes into
           * our strings across the wire without being escaped.
           * I think this is the protocol's bug, not thrift.js
           * } else if(ch === '/') {   // a single forward slash: /
           *  escapedString += '\\/';  // write out as \/
           * }
           */
        } else if (ch === '\b') {
          // a single backspace: invisible
          escapedString += '\\b'; // write out as: \b"
        } else if (ch === '\f') {
          // a single formfeed: invisible
          escapedString += '\\f'; // write out as: \f"
        } else if (ch === '\n') {
          // a single newline: invisible
          escapedString += '\\n'; // write out as: \n"
        } else if (ch === '\r') {
          // a single return: invisible
          escapedString += '\\r'; // write out as: \r"
        } else if (ch === '\t') {
          // a single tab: invisible
          escapedString += '\\t'; // write out as: \t"
        } else {
          escapedString += ch; // Else it need not be escaped
        }
      }
      this.tstack.push('"' + escapedString + '"');
    }
  }

  /** Serializes a string */
  public writeBinary(arg: string | Buffer | Uint8Array): void {
    let buf: Buffer;
    if (typeof arg === 'string') {
      buf = Buffer.from(arg, 'binary');
    } else if (arg instanceof Buffer) {
      buf = arg;
    } else if (arg instanceof Uint8Array) {
      buf = Buffer.from(arg);
    } else {
      throw new Error(
        'writeBinary called without a string/Buffer argument: ' + arg
      );
    }
    this.tstack.push('"' + buf.toString('base64') + '"');
  }

  /**
   * @class
   * @name AnonReadMessageBeginReturn
   * @property {string} fname - The name of the service method.
   * @property {Thrift.MessageType} mtype - The type of message call.
   * @property {number} rseqid - The sequence number of the message (0 in Thrift RPC).
   */
  /**
   * Deserializes the beginning of a message.
   * @returns {AnonReadMessageBeginReturn}
   */
  public readMessageBegin(): Thrift.TMessage {
    this.rstack = [];
    this.rpos = [];

    //Borrow the inbound transport buffer and ensure data is present/consistent
    const transBuf = this.trans.borrow();
    if (transBuf.readIndex >= transBuf.writeIndex) {
      throw new InputBufferUnderrunError();
    }
    let cursor = transBuf.readIndex;

    if (transBuf.buf[cursor] !== 0x5b) {
      //[
      throw new Error('Malformed JSON input, no opening bracket');
    }

    //Parse a single message (there may be several in the buffer)
    //  TODO: Handle characters using multiple code units
    cursor++;
    let openBracketCount = 1;
    let inString = false;
    for (; cursor < transBuf.writeIndex; cursor++) {
      const chr = transBuf.buf[cursor];
      //we use hexa charcode here because data[i] returns an int and not a char
      if (inString) {
        if (chr === 0x22) {
          //"
          inString = false;
        } else if (chr === 0x5c) {
          //\
          //escaped character, skip
          cursor += 1;
        }
      } else {
        if (chr === 0x5b) {
          //[
          openBracketCount += 1;
        } else if (chr === 0x5d) {
          //]
          openBracketCount -= 1;
          if (openBracketCount === 0) {
            //end of json message detected
            break;
          }
        } else if (chr === 0x22) {
          //"
          inString = true;
        }
      }
    }

    if (openBracketCount !== 0) {
      // Missing closing bracket. Can be buffer underrun.
      throw new InputBufferUnderrunError();
    }

    //Reconstitute the JSON object and consume the necessary bytes
    const robj = json_parse(
      transBuf.buf.slice(transBuf.readIndex, cursor + 1).toString()
    );
    this.trans.consume(cursor + 1 - transBuf.readIndex);

    if (!isArray(robj)) {
      throw new Thrift.TProtocolException(
        Thrift.TProtocolExceptionType.INVALID_DATA,
        'Expected parsed object to be an array'
      );
    }

    //Verify the protocol version
    const version = robj.shift();
    if (version != VERSION) {
      throw new Error('Wrong thrift protocol version: ' + version);
    }

    //Objectify the thrift message {name/type/sequence-number} for return
    // and then save the JSON object in rstack
    const fname = robj.shift();
    const mtype = robj.shift();
    const rseqid = robj.shift();
    this.rstack.push(robj.shift());
    return { fname, mtype, rseqid };
  }

  /** Deserializes the end of a message. */
  public readMessageEnd(): void {}

  /**
   * Deserializes the beginning of a struct.
   * @param {string} [name] - The name of the struct (ignored)
   * @returns {object} - An object with an empty string fname property
   */
  public readStructBegin(): Thrift.TStruct {
    //incase this is an array of structs
    if (this.rstack[this.rstack.length - 1] instanceof Array) {
      this.rstack.push(this.rstack[this.rstack.length - 1].shift());
    }

    return { fname: '' };
  }

  /** Deserializes the end of a struct. */
  public readStructEnd(): void {
    this.rstack.pop();
  }

  /**
   * @class
   * @name AnonReadFieldBeginReturn
   * @property {string} fname - The name of the field (always '').
   * @property {Thrift.Type} ftype - The data type of the field.
   * @property {number} fid - The unique identifier of the field.
   */
  /**
   * Deserializes the beginning of a field.
   * @returns {AnonReadFieldBeginReturn}
   */
  public readFieldBegin(): Thrift.TField {
    let fid = -1;
    let ftype = Thrift.Type.STOP;

    //get a fieldId
    for (const f in this.rstack[this.rstack.length - 1]) {
      if (f === null) {
        continue;
      }

      fid = parseInt(f, 10);
      this.rpos.push(this.rstack.length);

      const field = this.rstack[this.rstack.length - 1][fid];

      //remove so we don't see it again
      delete this.rstack[this.rstack.length - 1][fid];

      this.rstack.push(field);

      break;
    }

    if (fid != -1) {
      //should only be 1 of these but this is the only
      //way to match a key
      for (const i in this.rstack[this.rstack.length - 1]) {
        if (RType[i] === null) {
          continue;
        }

        ftype = RType[i];
        this.rstack[this.rstack.length - 1] = this.rstack[
          this.rstack.length - 1
        ][i];
      }
    }

    return { fname: '', ftype, fid };
  }

  /** Deserializes the end of a field. */
  public readFieldEnd(): void {
    const pos = safePopPosition(this.rpos);

    //get back to the right place in the stack
    while (this.rstack.length > pos) {
      this.rstack.pop();
    }
  }

  /**
   * @class
   * @name AnonReadMapBeginReturn
   * @property {Thrift.Type} ktype - The data type of the key.
   * @property {Thrift.Type} vtype - The data type of the value.
   * @property {number} size - The number of elements in the map.
   */
  /**
   * Deserializes the beginning of a map.
   * @returns {AnonReadMapBeginReturn}
   */
  public readMapBegin(): Thrift.TMap {
    let map = this.rstack.pop();
    let first = map.shift();
    if (first instanceof Array) {
      this.rstack.push(map);
      map = first;
      first = map.shift();
    }

    const ktype = RType[first];
    const vtype = RType[map.shift()];
    const size = map.shift();

    this.rpos.push(this.rstack.length);
    this.rstack.push(map.shift());

    return { ktype, vtype, size };
  }

  /** Deserializes the end of a map. */
  public readMapEnd(): void {
    this.readFieldEnd();
  }

  /**
   * @class
   * @name AnonReadColBeginReturn
   * @property {Thrift.Type} etype - The data type of the element.
   * @property {number} size - The number of elements in the collection.
   */
  /**
   * Deserializes the beginning of a list.
   * @returns {AnonReadColBeginReturn}
   */
  public readListBegin(): Thrift.TList {
    const list = this.rstack[this.rstack.length - 1];

    const etype = RType[list.shift()];
    const size = list.shift();

    this.rpos.push(this.rstack.length);
    this.rstack.push(list.shift());

    return { etype, size };
  }

  /** Deserializes the end of a list. */
  public readListEnd(): void {
    const pos = safePopPosition(this.rpos) - 2;
    const st = this.rstack;
    st.pop();
    if (st instanceof Array && st.length > pos && st[pos].length > 0) {
      st.push(st[pos].shift());
    }
  }

  /**
   * Deserializes the beginning of a set.
   * @returns {AnonReadColBeginReturn}
   */
  public readSetBegin(): Thrift.TSet {
    return this.readListBegin();
  }

  /** Deserializes the end of a set. */
  public readSetEnd(): void {
    return this.readListEnd();
  }

  public readBool(): boolean {
    return this.readValue() == '1';
  }

  public readByte(): number {
    return this.readI32();
  }

  public readI16(): number {
    return this.readI32();
  }

  public readI32(): number {
    return +this.readValue();
  }

  /** Returns the next value found in the protocol buffer */
  private readValue(f?: any): any {
    if (f === undefined) {
      f = this.rstack[this.rstack.length - 1];
    }

    let value;

    if (f instanceof Array) {
      if (f.length === 0) {
        value = undefined;
      } else {
        value = f.shift();
      }
    } else if (!(f instanceof Int64) && f instanceof Object) {
      for (const i in f) {
        if (i === null) {
          continue;
        }
        this.rstack.push(f[i]);
        delete f[i];

        value = i;
        break;
      }
    } else {
      value = f;
      this.rstack.pop();
    }

    return value;
  }

  public readI64(): Int64 {
    const n = this.readValue();
    if (typeof n === 'string') {
      // Assuming no one is sending in 1.11111e+33 format
      return Int64Util.fromDecimalString(n);
    } else {
      return new Int64(n);
    }
  }

  public readDouble(): number {
    return this.readI32();
  }

  public readBinary(): Buffer {
    return Buffer.from(this.readValue(), 'base64');
  }

  public readString(): string {
    return this.readValue();
  }
}

function safePopPosition(pos: number[]): number {
  const p = pos.pop();
  if (isNumber(p)) {
    return p;
  } else {
    throw new Thrift.TApplicationException(
      Thrift.TApplicationExceptionType.PROTOCOL_ERROR,
      'JSONProtocol error keeping track of position'
    );
  }
}
