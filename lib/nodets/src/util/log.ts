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

// Logger classes deal with the any type a lot, so disabling
/* eslint-disable @typescript-eslint/no-explicit-any */

/* eslint-disable-next-line @typescript-eslint/no-unused-vars */
function disabledFunction(message?: any, ...optionalParams: any[]): void {}

let logFunc = console.log;

let traceFunction = disabledFunction;
let debugFunction = disabledFunction;
let errorFunction = disabledFunction;
let warningFunction = disabledFunction;
let infoFunction = disabledFunction;

export enum LoggerLevel {
  trace = 'trace',
  debug = 'debug',
  error = 'error',
  warning = 'warning',
  info = 'info'
}

let logLevel: LoggerLevel = LoggerLevel.error; // default level

function factory(level: LoggerLevel): () => void {
  return function(...args: any[]) {
    const message = [`thrift: [${level.toUpperCase()}] `].concat(args);
    return logFunc(message);
  };
}

export function setLogFunc(
  func: (message?: any, ...optionalParams: any[]) => void
): void {
  logFunc = func;
}

export function setLogLevel(level: LoggerLevel): void {
  traceFunction = debugFunction = errorFunction = warningFunction = infoFunction = disabledFunction;
  logLevel = level;
  /* eslint-disable no-fallthrough */
  switch (logLevel) {
    case LoggerLevel.trace:
      traceFunction = factory(LoggerLevel.trace);
    case LoggerLevel.debug:
      debugFunction = factory(LoggerLevel.debug);
    case LoggerLevel.error:
      errorFunction = factory(LoggerLevel.error);
    case LoggerLevel.warning:
      warningFunction = factory(LoggerLevel.warning);
    case LoggerLevel.info:
      infoFunction = factory(LoggerLevel.info);
  }
  /* eslint-enable no-fallthrough */
}

export function getLogLevel(): LoggerLevel {
  return logLevel;
}

export function trace(message?: any, ...optionalParams: any[]): void {
  return traceFunction.apply(null, [message, ...optionalParams]);
}

export function debug(message?: any, ...optionalParams: any[]): void {
  return debugFunction.apply(null, [message, ...optionalParams]);
}

export function error(message?: any, ...optionalParams: any[]): void {
  return errorFunction.apply(null, [message, ...optionalParams]);
}

export function warning(message?: any, ...optionalParams: any[]): void {
  return warningFunction.apply(null, [message, ...optionalParams]);
}

export function info(message?: any, ...optionalParams: any[]): void {
  return infoFunction.apply(null, [message, ...optionalParams]);
}

// set default
setLogLevel(logLevel);
