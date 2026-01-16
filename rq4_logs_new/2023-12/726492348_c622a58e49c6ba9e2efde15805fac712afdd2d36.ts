import jwt from 'jsonwebtoken';
import { userEntity } from '../types/user';

/**
 * Generates a token for the given user.
 *
 * @param {userEntity} user - The user entity for whom the token is generated.
 * @return {Promise<string>} The generated token.
 */
export const genToken = async (user: userEntity): Promise<string> => {
  // Get the JWT secret key from the environment variables
  const configJwtKey: any = process.env.JWTSECRETKEY;

  // Sign the user's id and role into a JWT token using the secret key
  const token = await jwt.sign({ id: user.id, role: user.role }, configJwtKey, {
    algorithm: 'HS256',
    allowInsecureKeySizes: true,
    expiresIn: '10m', // 10 minutes
  });

  // Return the generated token
  return token;
};