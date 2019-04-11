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
import * as Thrift from './thrift';
export { Thrift };

export { setLogFunc, setLogLevel, getLogLevel } from './util/log';

export {
  Connection,
  createClient,
  createConnection,
  createUDSConnection,
  createSSLConnection,
  createStdIOClient,
  createStdIOConnection
} from './client/connection';

export {
  HttpConnection,
  createHttpConnection,
  createHttpUDSConnection,
  createHttpClient
} from './client/http_connection';

export {
  WSConnection,
  createWSConnection,
  createWSClient
} from './client/ws_connection';

export {
  XHRConnection,
  createXHRConnection,
  createXHRClient
} from './client/xhr_connection';

export { createServer, createMultiplexServer } from './server/server';

export { createWebServer } from './server/web_server';

import Int64 from 'node-int64';
export { Int64 };

import Q from 'q';
export { Q };

export { MultiplexedProcessor } from './server/multiplexed_processor';
import Multiplexer from './client/multiplexed_protocol';
export { Multiplexer };

export { TFramedTransport, TBufferedTransport } from './transport';
export { TBinaryProtocol, TJSONProtocol, TCompactProtocol } from './protocol';
