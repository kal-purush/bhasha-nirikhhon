export interface apiUrls {
  readonly basePath: string;
  login(): string;
  todosByUserId(userId: string): string;
  createTodo(): string;
  createUser(): string;
}

export class ClientApiUrls implements apiUrls {
  readonly basePath: string;

  constructor() {
    this.basePath = "/api";
  }

  login = () => `${this.basePath}/auth/login`;
  todosByUserId = (userId: string) => {
    return `${this.basePath}/todos?userId=${userId}`;
  };
  createTodo = () => `${this.basePath}/todos`;
  createUser = () => `${this.basePath}/auth/signup`;
}

export class ServerApiUrls extends ClientApiUrls {
  readonly basePath: string;

  constructor() {
    super();
    this.basePath = process.env.HOST ?? "";
  }

  login = () => `${this.basePath}/users/login`;
  createUser = () => `${this.basePath}/users`;
}

export const getApiUrls = () =>
  process.env.HOST ? new ServerApiUrls() : new ClientApiUrls();