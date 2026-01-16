export interface Draw {
  gameAddress: string;
  hashPrev: string;
  code: string;
}

export const SpecialType = {
  Special : {
    gameAddress: 'address',
    hashPrev: 'bytes32',
    code: 'string',
  }
};