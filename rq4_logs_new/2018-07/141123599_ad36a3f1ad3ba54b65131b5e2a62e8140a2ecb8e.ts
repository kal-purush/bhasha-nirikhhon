import * as bcrypt from 'bcrypt';
import * as jwt from 'jsonwebtoken';
// tslint:disable-next-line
require('dotenv').config();

const secret = process.env.JWT_SECRET;

export const verifyPassword = (password: string, hashedPassword: string): boolean => {
  if (!password || !hashedPassword) {
    return false;
  }
  return bcrypt.compareSync(password, hashedPassword);
};

export const signToken = ({ id }) => {
  return jwt.sign({
    userId: id,
  }, secret, { expiresIn: '12h' });
};