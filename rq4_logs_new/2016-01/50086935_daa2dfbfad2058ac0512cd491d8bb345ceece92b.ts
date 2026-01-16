'use strict';

interface TokenData {
  auth_token: string,
  refresh_token?: string,
  expires?: number
}

export = TokenData;