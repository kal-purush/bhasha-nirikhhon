import * as bcrypt from "bcrypt";

export namespace BCrypt {
  const rounds = 12;

  export const hash = (password: string) => bcrypt.hash(password, rounds);

  export const verify = (password: string, hash: string) =>
    bcrypt.compare(password, hash);
}