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

import TAbstractTransport from './abstract_transport';
import TBufferedTransport from './buffered_transport';
import TFramedTransport from './framed_transport';

export { TAbstractTransport, TBufferedTransport, TFramedTransport };

/**
 * @interface
 * Describes a generic transport constructor, along with the attached static methods
 * Used to describe a transport constructor as a passed type, rather than an instance
 * of a class
 */
export interface TTransportClass<TTransport extends TAbstractTransport> {
  new (...args: any[]): TTransport;
  receiver(
    callback: (transport: TTransport, seqid: number) => void,
    seqid: number
  ): (data: Buffer) => void;
}
