import { expect } from 'chai';
import { Request } from 'express';
import { getToken } from '../../src/helper';

export default () => {
    // Arrange
  const baseRequest = {
    headers: {},
    query: {},
    header (name: string): string | undefined { return this.headers[name]; },
  } as any as Request;

  it('should return the token if provided via query', () => {
      // Arrange
    const request = Object.assign({}, baseRequest, { query: { api_key: 'query_token' } });

    // Act
    const token = getToken(request);

    // Assert
    expect(token).to.equal('query_token');
  });

  it('should return the token if provided via the header', () => {
    // Arrange
    const request = Object.assign({}, baseRequest, { headers: { 'X-API-TOKEN': 'header_token' } });

    // Act
    const token = getToken(request);

    // Assert
    expect(token).to.equal('header_token');
  });

  it('should return undefined if none of the above are provided', () => {
    // Arrange
    const request = Object.assign({}, baseRequest);

    // Act
    const token = getToken(request);

    // Assert
    expect(token).to.equal(undefined);
  });
};