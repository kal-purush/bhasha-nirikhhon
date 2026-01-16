// src/utils/jwt.utils.js
import jwt from 'jsonwebtoken';

const generateToken = (user) => {
  const payload = { id: user._id };
  const secret = process.env.JWT_SECRET;
  console.log('JWT Secret (signing):', secret); // Debugging line
  const options = { expiresIn: '1h' };
  return jwt.sign(payload, secret, options);
};

export { generateToken };