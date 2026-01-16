import { UserDatabase } from "../data/UserDatabase";
import { CustomError, InvalidEmail, InvalidPassword, RequiredToken } from "../error/customError";
import {
  UserInputDTO,
  user,
} from "../model/user";
import { GenerateId } from "../services/GenerateId";
import { GenerateToken } from "../services/GenerateToken";
import { HashManager } from "../services/HashManager";

export class UserBusiness {
  public singup = async (input: UserInputDTO): Promise<string> => {
    try {
      const { email, password } = input;

      if (!email || !password) {
        throw new CustomError(
          400,
          'Preencha os campos "email" e "password"'
        );
      }

      if (!email.includes("@")) {
        throw new InvalidEmail();
      }

      if (password.length < 6) {
        throw new InvalidPassword();
      }

      const newId = new GenerateId();
      const id: string = newId.generateId();

      const hashManager = new HashManager();
      const newPassword = await hashManager.hash(password)

      const user: user = {
        id,
        email,
        password: newPassword,
      };
      const userDatabase = new UserDatabase();
      await userDatabase.insertUser(user);

      const generateToken = new GenerateToken() 
      const token = generateToken.generate({id})
      return token

    } catch (error: any) {
      throw new CustomError(400, error.message);
    }
  };

  public login = async (input: UserInputDTO) => {
    try {
      const { email, password } = input;

      if (!email || !password) {
        throw new CustomError(
          400,
          'Preencha os campos "email" e "password"'
        );
      }

      if (!email.includes("@")) {
        throw new InvalidEmail();
      }

      if (password.length < 6) {
        throw new InvalidPassword();
      }

      const userDatabase = new UserDatabase();
      const userData: user = await userDatabase.getUser(email);

      const hashManager = new HashManager();
      const comparePassword = await hashManager.compare(password, userData.password)

      if(!comparePassword) {
        throw new InvalidPassword();
      }

      const {id} = userData

      const generateToken = new GenerateToken() 
      const token = generateToken.generate({id})
      return token

    } catch (error: any) {
      throw new CustomError(400, error.message);
    }
  }

  public getUser = async (token: string) => {
    try {

      if(!token){
        throw new RequiredToken();
      }


      const generateToken = new GenerateToken()
      const tokenInfo = generateToken.compare(token)
      
      const userDatabase = new UserDatabase();
      const userInfo = await userDatabase.getUserById(tokenInfo.id)

      return userInfo
    } catch (error: any) {
      throw new CustomError(400, error.message);
    }
  }
}