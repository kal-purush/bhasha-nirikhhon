// tslint:disable
// graphql typescript definitions

declare namespace GQL {
  interface IGraphQLResponseRoot {
    data?: IQuery | IMutation;
    errors?: Array<IGraphQLResponseError>;
  }

  interface IGraphQLResponseError {
    /** Required for all errors */
    message: string;
    locations?: Array<IGraphQLResponseErrorLocation>;
    /** 7.2.2 says 'GraphQL servers may provide additional entries to error' */
    [propName: string]: any;
  }

  interface IGraphQLResponseErrorLocation {
    line: number;
    column: number;
  }

  interface IQuery {
    __typename: 'Query';
    me: IUser | null;
    users: Array<IUser | null> | null;
    user: IUser | null;
  }

  interface IUserOnQueryArguments {
    id?: string | null;
  }

  interface IUser {
    __typename: 'User';
    id: string;
    name: string;
    email: string;
    permissions: Array<Permission | null> | null;
  }

  const enum Permission {
    ADMIN = 'ADMIN',
    USER = 'USER'
  }

  interface IMutation {
    __typename: 'Mutation';
    signUp: IAuthResponse;
    logIn: IAuthResponse;
    deleteMe: IUser | null;
  }

  interface ISignUpOnMutationArguments {
    name: string;
    email: string;
    password: string;
  }

  interface ILogInOnMutationArguments {
    email: string;
    password: string;
  }

  interface IAuthResponse {
    __typename: 'AuthResponse';
    id: string | null;
    name: string | null;
    email: string | null;
    token: string | null;
    errors: Array<IError | null> | null;
  }

  interface IError {
    __typename: 'Error';
    path: string;
    message: string;
  }
}

// tslint:enable