import { faker } from '@faker-js/faker';
import {
  IAuthRequest,
  IUserPayload,
  IUserToken,
} from '../../interfaces/service.interface';
import { CredentialsDto } from '../../dto/credentials.dto';

export const MockUserCredentials: CredentialsDto = {
  email: faker.internet.email(),
  password: faker.internet.password(),
};

export const MockUserPayload: IUserPayload = {
  id: faker.number.int(),
  name: faker.person.fullName(),
  email: MockUserCredentials.email,
};

export const MockAuthRequest: IAuthRequest = {
  user: MockUserPayload,
} as IAuthRequest;

export const MockAccessToken: IUserToken = {
  accessToken: faker.string.alphanumeric(),
};