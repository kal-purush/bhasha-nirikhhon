
export interface TokenAttribute {
  name: string;
  value: string;
}

export interface Token {
  tokenId: number;
  name: string;
  description: string;
  imageUrl: string;
  attributes: TokenAttribute[];
  attributeMap: Record<string, string>;
}

export interface TokenCollectionAttributeValue {
  name: string;
  tokenIds: number[];
}

export interface TokenCollectionAttribute {
  name: string;
  tokenIds: number[];
  values: Record<string, TokenCollectionAttributeValue>;
}

export interface TokenCollection {
  tokens: Record<number, Token>;
  attributes: Record<string, TokenCollectionAttribute>;
}