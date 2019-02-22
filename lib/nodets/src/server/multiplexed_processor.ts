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
import * as Thrift from '../thrift';
import { TAbstractProtocol } from 'src/protocol';

export class MultiplexedProcessor {
  public services: { [key: string]: Thrift.ServiceProcessor } = {};

  public registerProcessor(
    name: string,
    handler: Thrift.ServiceProcessor
  ): void {
    this.services[name] = handler;
  }

  public process(input: TAbstractProtocol, output: TAbstractProtocol): void {
    const begin = input.readMessageBegin();

    if (
      begin.mtype != Thrift.MessageType.CALL &&
      begin.mtype != Thrift.MessageType.ONEWAY
    ) {
      throw new Thrift.TException(
        'TMultiplexedProcessor: Unexpected message type'
      );
    }

    const [serviceName, functionName] = begin.fname.split(':');

    if (!(serviceName in this.services)) {
      throw new Thrift.TException(
        `TMultiplexedProcessor: Unknown service: ${serviceName}`
      );
    }

    // construct a proxy object which stubs the readMessageBegin
    // for the service. In the future we'd like to use native JS
    // Proxy objects but those don't have suitable polyfills
    const inputProxy = Object.create(Object.getPrototypeOf(input));
    Object.keys(input).forEach(key => {
      // @ts-ignore TAbstractProtocol has no index signature. This is fine.
      inputProxy[key] = input[key];
    });
    // Override the inherited prototype method
    inputProxy.readMessageBegin = function() {
      return {
        fname: functionName,
        mtype: begin.mtype,
        rseqid: begin.rseqid
      };
    };

    this.services[serviceName].process(inputProxy as TAbstractProtocol, output);
  }
}
