import jwt from 'jsonwebtoken';
import jwksClient from 'jwks-rsa';

const client = jwksClient({
  jwksUri: 'https://zhammer.auth0.com/.well-known/jwks.json'
});

function getKey(header: any, cb: any) {
  client.getSigningKey(header.kid, (err, key) => {
    const signingKey = key.publicKey || key.rsaPublicKey;
    cb(null, signingKey);
  });
}

const jwtOptions = {
  audience: 'https://zhammer.auth0.com/api/v2/',
  issuer: 'https://zhammer.auth0.com/',
  algorithms: ['RS256']
};

/**
 * Parses a one-thing auth0 accesstoken and returns an object with the 'sub' id
 * of the user in auth0.
 * @param accessToken Authorization header value.
 */
export async function parseJWT(accessToken: string): Promise<{ sub: string }> {
  return await new Promise((resolve, reject) => {
    jwt.verify(accessToken!, getKey, jwtOptions, (err, decoded) => {
      if (err) {
        return reject(err);
      }
      resolve(decoded as { sub: string });
    });
  });
}