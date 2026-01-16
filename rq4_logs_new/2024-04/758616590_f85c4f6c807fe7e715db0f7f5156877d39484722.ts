import * as crypto from 'crypto';
const SECRET_KEY = process.env.SECRET_KEY!;

export const generateHash = (password: string) => {
  const random = () => crypto.randomBytes(128).toString('base64');
  const salt = random();

  const createHash = (password, salt) => {
    return crypto
      .createHmac('sha256', salt + password)
      .update(SECRET_KEY)
      .digest('hex');
  };

  return {
    salt: salt,
    hash: createHash(password, salt),
  };
};

export const validateHash = (password: string, salt: string, hash: string) => {
  const createHash = (password: string, salt: string) => {
    return crypto
      .createHmac('sha256', salt + password)
      .update(SECRET_KEY)
      .digest('hex');
  };

  const newHash = createHash(password, salt);

  const isValid = newHash == hash;
  return isValid;
};