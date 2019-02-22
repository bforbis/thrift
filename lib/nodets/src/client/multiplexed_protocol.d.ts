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

// Multiplexed protocol is very dynamic, and thus difficult to fully
// convert to typescript. Easier to treat as a black box for now

import {
  TConnection,
  ServiceClient,
  ThriftService,
  StaticClass
} from '../thrift';
import { TAbstractProtocol } from '../protocol';

export default class Multiplexer<TProtocol extends TAbstractProtocol> {
  public seqid: number;
  public createClient(
    serviceName: string,
    ServiceClient: ThriftService | StaticClass<ServiceClient>,
    connection: TConnection
  ): TProtocol;
}
