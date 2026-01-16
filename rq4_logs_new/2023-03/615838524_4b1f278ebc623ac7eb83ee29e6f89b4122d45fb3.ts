import * as bcrypt from 'bcrypt';

/**
 * Hashes a password
 *
 * @param {string} password - the password string to be hashed
 * @returns {Promise<string>} the hashed pasesword
 */
export async function hashPassword(password: string): Promise<string> {

  const saltRounds = 10;
  const hash = await bcrypt.hash(password, saltRounds);

  return hash;
}

/**
 * Checks if plain password matches the hash
 * 
 * @param {string} password - the plain password
 * @param {string} hash - the password hash
 * @returns {Promise<boolean>} true if the password matches else false
 */
export async function comparePassword(password: string, hash: string): Promise<boolean> {
  return await bcrypt.compare(password, hash);
}