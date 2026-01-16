import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import { getCustomRepository } from "typeorm";

import AppError from "../../../shared/errors/AppError";
import Colaboradores from "../typeorm/entity";
import ColaboradoresRepository from "../typeorm/repository";

interface IResponse {
  user: Colaboradores;
  token: string;
}

interface ILogin {
  username: string;
  password: string;
}

class LoginService {
  public async execute({ username, password }: ILogin): Promise<IResponse> {
    const userRepository = getCustomRepository(ColaboradoresRepository);
    const user = await userRepository.findOne({ username });

    console.log(user);
    if (!user) throw new AppError("UserName/Password incorrect");

    const isValid = await bcrypt.compare(password, user.password);

    console.log(isValid);

    if (!isValid) throw new AppError("UserName/Password incorrect");

    const token = jwt.sign({}, "3bc3e48365fd68eea05feca17b43ee7f", {
      subject: user.id,
      expiresIn: "1d",
    });

    return { user, token };
  }
}

export default new LoginService();