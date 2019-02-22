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

import { TAbstractTransport } from './transport';
import { TAbstractProtocol } from './protocol';

//  Legacy inherits method for code < ES6
export { inherits } from 'util';

// Re-export imported modules
export {
  TApplicationException,
  TApplicationExceptionType,
  TProtocolException,
  TProtocolExceptionType,
  TException
} from './exception';

export enum Type {
  STOP = 0,
  VOID = 1,
  BOOL = 2,
  BYTE = 3,
  I08 = 3,
  DOUBLE = 4,
  I16 = 6,
  I32 = 8,
  I64 = 10,
  STRING = 11,
  UTF7 = 11,
  STRUCT = 12,
  MAP = 13,
  SET = 14,
  LIST = 15,
  UTF8 = 16,
  UTF16 = 17
}
export type OnFlushCallback = (msg: Buffer, seqid?: number | null) => void;

export interface TransportState {
  buf: Buffer;
  readIndex: number;
  writeIndex: number;
}

export interface TMap {
  ktype: Type;
  vtype: Type;
  size: number;
}

export interface TMessage {
  fname: string;
  mtype: MessageType;
  rseqid: number;
}

export interface TField {
  fname: string;
  ftype: Type;
  fid: number;
}

export interface TList {
  etype: Type;
  size: number;
}

export interface TSet {
  etype: Type;
  size: number;
}

export interface TStruct {
  fname: string;
}

export interface TStructLike {
  read(input: TAbstractProtocol): void;
  write(output: TAbstractProtocol): void;
}

export enum MessageType {
  CALL = 1,
  REPLY = 2,
  EXCEPTION = 3,
  ONEWAY = 4
}

// Represents a generic class constructor
export interface StaticClass<T> {
  new (...args: any[]): T;
}

// Interfaces for generated service code
export interface ThriftService {
  Client: StaticClass<ServiceClient>;
  Processor: StaticClass<ServiceProcessor>;
}

export interface ServiceClient {
  output: Buffer;
  pClass: any;
  seqid(): number;
  new_seqid(): number;

  // Maps sequence id to request callback
  _reqs: {
    [keyid: number]: (error: any, result: any) => void;
  };
}
export interface ServiceProcessor {
  process(input: TAbstractProtocol, output: TAbstractProtocol): void;
}

export interface TConnection {
  write(buf: Buffer, seqid: number): void;
  client?: ServiceClient;
  transport: StaticClass<TAbstractTransport>;
  protocol: StaticClass<TAbstractProtocol>;
}

// Utility Functions.. Consider moving to a utils file

export function objectLength(obj: object): number {
  return Object.keys(obj).length;
}

/* eslint-disable @typescript-eslint/no-explicit-any */
export function copyList(lst?: any[], types?: any): any[] | undefined {
  if (lst === undefined) {
    return lst;
  }

  const type = Array.isArray(types) ? types[0] : types;
  const Type = type;

  const result: any[] = [];
  lst.forEach(val => {
    if (type === null || type === undefined) {
      result.push(val);
    } else if (type === copyMap || type === copyList) {
      result.push(type(val, types.slice(1)));
    } else {
      result.push(new Type(val));
    }
  });
  return result;
}

export function copyMap(
  obj?: { [key: string]: any },
  types?: any
): object | undefined {
  if (obj === undefined) {
    return obj;
  }

  const type = Array.isArray(types) ? types[0] : types;
  const Type = type;

  const result: { [key: string]: any } = {};
  Object.keys(obj).forEach(prop => {
    const val: any = obj[prop];
    if (type === null || type === undefined) {
      result[prop] = val;
    } else if (type === copyMap || type === copyList) {
      result[prop] = type(val, types.slice(1));
    } else {
      result[prop] = new Type(val);
    }
  });
  return result;
}
/* eslint-enable @typescript-eslint/no-explicit-any */
