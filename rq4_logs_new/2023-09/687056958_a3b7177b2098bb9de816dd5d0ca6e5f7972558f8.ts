import crypto from 'crypto'

const salt = crypto.randomBytes(16).toString('hex'); // Em uma aplicação não ficaria exposto e sim nas variaveis de ambiente

type DbPassword = {
  hash: string,
  salt: string
}

const cryptoPassword = (password: string): DbPassword => {
  const hash = crypto
  .pbkdf2Sync(password, salt, 1000, 64, 'sha512')
  .toString('hex');
return { hash, salt };
}

const verifycryptoPassword = (insertedPassword: string, hashPassword: string, salt: string): boolean => {
  const hashSenhaInserida = crypto
  .pbkdf2Sync(insertedPassword, salt, 1000, 64, 'sha512')
  .toString('hex');

  return hashSenhaInserida === hashPassword;
}

export default { cryptoPassword, verifycryptoPassword
}