import fastify from 'fastify';
import common from '../lib/common';
import { ConfigType } from '../src/config';
/// <reference path="../lib/db.d.ts" />

declare global {
  namespace app {
    type Sandbox = {
      api: Record<string, any>;
      common: typeof common;
      db: (table: string) => typeof db;
      config: ConfigType;
      schemas: {
        api: Record<string, any>;
      };
    };

    type RPCMessage = {
      jsonrpc: '2.0';
      name: string;
      method: string;
      params?: any;
      id?: string;
    };
  }
}