export interface AuthCredentials {
  email: string;
  password: string;
}

export interface Role {
  id: number;
  value: string;
}

export interface User {
  readonly id: string;
  readonly email: string;
  readonly username: string | null;
  readonly auth_type: string;
  readonly banned: boolean;
  readonly role: Role;
  readonly country: string | null;
  readonly photo: string | null;
  readonly mail_subscribe: boolean | null;
}

export interface AuthResponse {
  refresh_token: string;
  access_token: string;
  user: User;
}

export interface Token {
  id: number;
  user_id: string;
  refresh_token: string;
}