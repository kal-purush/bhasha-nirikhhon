import {
  ICreateTodo,
  ICreateUser,
  ITodo,
  IUser,
  IUserCredentials,
} from "shared";
import { getJson, postJson, Result } from "../lib/http.client";

export class ClientApi {
  readonly basePath: string;

  constructor() {
    this.basePath = "/api";
  }

  doLogin(login: IUserCredentials): Promise<Result<IUser>> {
    return postJson<IUserCredentials, IUser>(
      `${this.basePath}/auth/login`,
      login
    );
  }

  getTodos(): Promise<Result<ITodo[]>> {
    return getJson<ITodo[]>(`${this.basePath}/todos`);
  }

  createTodo(createTodo: ICreateTodo): Promise<Result<ITodo>> {
    return postJson<ICreateTodo, ITodo>(`${this.basePath}/todos`, createTodo);
  }

  createUser(createUser: ICreateUser): Promise<Result<IUser>> {
    return postJson<ICreateUser, IUser>(
      `${this.basePath}/auth/signup`,
      createUser
    );
  }
}