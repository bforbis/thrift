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

import Int64 from 'node-int64';
import * as Thrift from '../thrift';
import { TAbstractTransport } from '../transport';

export default abstract class TAbstractProtocol {
  protected trans: TAbstractTransport;
  protected _seqid: null | number;

  public constructor(trans: TAbstractTransport) {
    this.trans = trans;
    this._seqid = null;
  }

  // Abstract Write Methods

  public abstract writeMessageBegin(
    name: string,
    type: Thrift.MessageType,
    seqid: number
  ): void;
  public abstract writeMessageEnd(): void;
  public abstract writeStructBegin(name: string): void;
  public abstract writeStructEnd(): void;
  public abstract writeFieldBegin(
    name: string,
    type: Thrift.Type,
    id: number
  ): void;
  public abstract writeFieldEnd(): void;
  public abstract writeFieldStop(): void;
  public abstract writeMapBegin(
    ktype: Thrift.Type,
    vtype: Thrift.Type,
    size: number
  ): void;
  public abstract writeMapEnd(): void;
  public abstract writeListBegin(etype: Thrift.Type, size: number): void;
  public abstract writeListEnd(): void;
  public abstract writeSetBegin(etype: Thrift.Type, size: number): void;
  public abstract writeSetEnd(): void;
  public abstract writeBool(bool: boolean): void;
  public abstract writeByte(b: number): void;
  public abstract writeI16(i16: number): void;
  public abstract writeI32(i32: number): void;
  public abstract writeI64(i64: number | Int64): void;
  public abstract writeDouble(dub: number): void;
  public abstract writeString(arg: string | Buffer | Uint8Array): void;
  public abstract writeBinary(arg: string | Buffer | Uint8Array): void;

  // Abstract Read Methods

  public abstract readMessageBegin(): Thrift.TMessage;
  public abstract readMessageEnd(): void;
  public abstract readStructBegin(): Thrift.TStruct;
  public abstract readStructEnd(): void;
  public abstract readFieldBegin(): Thrift.TField;
  public abstract readFieldEnd(): void;
  public abstract readMapBegin(): Thrift.TMap;
  public abstract readMapEnd(): void;
  public abstract readListBegin(): Thrift.TList;
  public abstract readListEnd(): void;
  public abstract readSetBegin(): Thrift.TSet;
  public abstract readSetEnd(): void;
  public abstract readBool(): boolean;
  public abstract readByte(): number;
  public abstract readI16(): number;
  public abstract readI32(): number;
  public abstract readI64(): Int64;
  public abstract readDouble(): number;
  public abstract readBinary(): Buffer;
  public abstract readString(): string;

  // Inheritable Methods

  public flush(): void {
    return this.trans.flush();
  }

  public getTransport(): TAbstractTransport {
    return this.trans;
  }

  public skip(type: Thrift.Type): void {
    let mapBegin: Thrift.TMap, setBegin: Thrift.TSet, listBegin: Thrift.TList;
    switch (type) {
      case Thrift.Type.STOP:
        return;
      case Thrift.Type.BOOL:
        this.readBool();
        break;
      case Thrift.Type.BYTE:
        this.readByte();
        break;
      case Thrift.Type.I16:
        this.readI16();
        break;
      case Thrift.Type.I32:
        this.readI32();
        break;
      case Thrift.Type.I64:
        this.readI64();
        break;
      case Thrift.Type.DOUBLE:
        this.readDouble();
        break;
      case Thrift.Type.STRING:
        this.readString();
        break;
      case Thrift.Type.STRUCT:
        this.readStructBegin();
        while (true) {
          const r = this.readFieldBegin();
          if (r.ftype === Thrift.Type.STOP) {
            break;
          }
          this.skip(r.ftype);
          this.readFieldEnd();
        }
        this.readStructEnd();
        break;
      case Thrift.Type.MAP:
        mapBegin = this.readMapBegin();
        for (let i = 0; i < mapBegin.size; ++i) {
          this.skip(mapBegin.ktype);
          this.skip(mapBegin.vtype);
        }
        this.readMapEnd();
        break;
      case Thrift.Type.SET:
        setBegin = this.readSetBegin();
        for (let i2 = 0; i2 < setBegin.size; ++i2) {
          this.skip(setBegin.etype);
        }
        this.readSetEnd();
        break;
      case Thrift.Type.LIST:
        listBegin = this.readListBegin();
        for (let i3 = 0; i3 < listBegin.size; ++i3) {
          this.skip(listBegin.etype);
        }
        this.readListEnd();
        break;
      default:
        throw new Error('Invalid type: ' + type);
    }
  }
}
