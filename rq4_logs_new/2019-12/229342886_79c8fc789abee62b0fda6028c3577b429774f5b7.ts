import * as ts from 'typescript'
declare global {
  namespace NodeJS {
    interface ProcessEnv {
      NODE_ENV: 'development' | 'production' | 'testing';
      PORT?: string;
    }
  }
}