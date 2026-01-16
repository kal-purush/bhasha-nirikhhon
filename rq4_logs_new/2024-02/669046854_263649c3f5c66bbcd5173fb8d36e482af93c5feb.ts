import crypto from "crypto";

export interface IHashedPassword {
  salt: Buffer;
  hashedPassword: Buffer;
}

export const hashPassword = async (
  password: string,
): Promise<IHashedPassword> => {
  return new Promise((resolve, reject) => {
    const salt = crypto.randomBytes(16);
    crypto.pbkdf2(
      password,
      salt,
      310000,
      32,
      "sha256",
      (err, hashedPassword) => {
        if (err) reject(err);
        resolve({ salt, hashedPassword });
      },
    );
  });
};