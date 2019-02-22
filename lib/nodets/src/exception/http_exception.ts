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
  TApplicationException,
  TApplicationExceptionType
} from './application_exception';
import { IncomingMessage } from 'http';

export default class THTTPException extends TApplicationException {
  public statusCode?: number;
  public response: IncomingMessage;

  public constructor(response: IncomingMessage) {
    const message = `Received a response with a bad HTTP status code: ${
      response.statusCode
    }`;
    super(TApplicationExceptionType.PROTOCOL_ERROR, message);
    Error.captureStackTrace(this, this.constructor);
    this.statusCode = response.statusCode;
    this.response = response;
  }
}
