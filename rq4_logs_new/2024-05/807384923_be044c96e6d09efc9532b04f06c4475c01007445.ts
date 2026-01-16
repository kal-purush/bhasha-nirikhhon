import "sst/node/config";
declare module "sst/node/config" {
  export interface ConfigTypes {
    APP: string;
    STAGE: string;
  }
}

import "sst/node/api";
declare module "sst/node/api" {
  export interface ApiResources {
    "api": {
      url: string;
    }
  }
}

import "sst/node/config";
declare module "sst/node/config" {
  export interface SecretResources {
    "DB_HOST": {
      value: string;
    }
  }
}

import "sst/node/config";
declare module "sst/node/config" {
  export interface SecretResources {
    "DB_USER": {
      value: string;
    }
  }
}

import "sst/node/config";
declare module "sst/node/config" {
  export interface SecretResources {
    "DB_PASSWORD": {
      value: string;
    }
  }
}

import "sst/node/api";
declare module "sst/node/api" {
  export interface ApiResources {
    "prisma-api": {
      url: string;
    }
  }
}
