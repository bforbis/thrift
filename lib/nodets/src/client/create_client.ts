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

import {
  ThriftService,
  ServiceClient,
  TConnection,
  StaticClass
} from '../thrift';

/**
 * Creates a new client object for the specified Thrift service.
 * @param {object} ServiceClient - The module containing the generated service client
 * @param {Connection} Connection - The connection to use.
 * @returns {object} The client object.
 */
export default function createClient(
  ServiceClient: ThriftService | StaticClass<ServiceClient>,
  connection: TConnection
): ServiceClient {
  // TODO validate required options and throw otherwise
  if (isThriftService(ServiceClient)) {
    ServiceClient = ServiceClient.Client;
  }
  // TODO detangle these initialization calls
  // creating "client" requires
  //   - new service client instance
  //
  // New service client instance requires
  //   - new transport instance
  //   - protocol class reference
  //
  // New transport instance requires
  //   - Buffer to use (or none)
  //   - Callback to call on flush

  // Wrap the write method
  const writeCb = function(buf: Buffer, seqid: number): void {
    connection.write(buf, seqid);
  };
  const transport = new connection.transport(undefined, writeCb);
  const client = new ServiceClient(transport, connection.protocol);
  transport.client = client;
  connection.client = client;
  return client;
}

/**
 * Typeguard for figuring out if an object is the whole generated
 * JS service module, or just the client part
 * @param {object} service - Passed in generated service code
 * @returns {boolean} - Determines if object is the entire module, or just the client class
 */
function isThriftService(
  service: ThriftService | StaticClass<ServiceClient>
): service is ThriftService {
  return (service as ThriftService).Client !== undefined;
}
