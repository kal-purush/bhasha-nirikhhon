import * as sinon from 'sinon';
import Container from 'typedi';
import { getRepository, Repository } from 'typeorm';
import { expect } from 'chai';
import { gql } from 'apollo-server-express';
import { UserEntity } from '@data/entities';
import { postGraphQL } from '@test/request-maker';
import { CryptoService } from '@core/crypto.service';
import { UserInputModel } from '@domain/model';
import { CreateUserResponse } from './create-user.response';
import { CreateUserInput } from './create-user.input';
import { StatusCode } from '@core/error/error.type';

describe('GraphQL - UserResolver - Create', () => {
  let repository: Repository<UserEntity>;
  let cryptoService: CryptoService;

  const createUserMutation = gql`
    mutation createUser($data: CreateUserInput!) {
      createUser(data: $data) {
        id
        name
        email
      }
    }
  `;

  before(() => {
    repository = getRepository(UserEntity);
    cryptoService = Container.get(CryptoService);
  });

  afterEach(async () => {
    sinon.restore();
    await repository.delete({});
  });

  it('should return the created user', async () => {
    const mockSalt = 'Random salt';
    const password = 'p1ssWord';
    const email = 'taq@hotmail.com';

    const input: UserInputModel = {
      email,
      password,
      name: 'taqTest',
    };

    sinon.stub(cryptoService, 'generateRandomPassword').callsFake(() => mockSalt);
    const response = await postGraphQL<{ createUser: CreateUserResponse }>(createUserMutation, { data: input });
    const data = response.body.data?.createUser;
    expect(data).to.be.not.empty;

    const userDb = await repository.findOne({ email });
    expect(userDb?.id).to.be.not.empty;
    expect(userDb?.name).to.be.eq(input.name);
    expect(userDb?.email).to.be.eq(input.email);
    expect(userDb?.salt).to.be.eq(mockSalt);
    expect(userDb?.password).to.be.eq(cryptoService.generateHashWithSalt(password, mockSalt));

    expect(data).to.be.deep.eq({ id: userDb?.id, name: userDb?.name, email: userDb?.email });
  });

  it('should give error if e-mail is already taken', async () => {
    const email = 'Taq@taqtile.com.br';
    const user = await repository.save({
      email,
      name: 'Taq',
      salt: 'salt',
      password: '123',
    });

    const input: CreateUserInput = {
      email,
      password: 'p1ssWord',
      name: 'User Test',
    };

    const { body } = await postGraphQL<{ createUser: CreateUserResponse }>(createUserMutation, { data: input });
    expect(body.data).to.be.null;
    expect(body.errors?.[0].code).to.be.eq(StatusCode.BadRequest);
    expect(body.errors?.[0].message).to.be.eq('this email already exists');

    const usersDb = await repository.find({ email: user.email });
    expect(usersDb).to.have.lengthOf(1);

    const userDb = usersDb[0];
    expect(userDb.id).to.be.eq(user.id);
    expect(userDb.name).to.be.eq(user.name);
    expect(userDb.salt).to.be.eq(user.salt);
    expect(userDb.password).to.be.eq(user.password);
  });

  it(`should give error with proper message if password doesn't follow the rules`, async () => {
    const input: CreateUserInput = {
      email: 'user@taqtile.com.br',
      password: '',
      name: 'User Test',
    };

    let response = await postGraphQL<{ createUser: CreateUserResponse }>(createUserMutation, { data: input });
    expect(response.body.data).to.be.null;
    expect(response.body.errors?.[0].code).to.be.eq(StatusCode.BadRequest);
    expect(response.body.errors?.[0].message).to.be.eq('user.error.password.too-short');

    input.password = 'abcdefgh';
    response = await postGraphQL<{ createUser: CreateUserResponse }>(createUserMutation, { data: input });
    expect(response.body.data).to.be.null;
    expect(response.body.errors?.[0].code).to.be.eq(StatusCode.BadRequest);
    expect(response.body.errors?.[0].message).to.be.eq('user.error.password.no-digit');

    input.password = '1234567';
    response = await postGraphQL<{ createUser: CreateUserResponse }>(createUserMutation, { data: input });
    expect(response.body.data).to.be.null;
    expect(response.body.errors?.[0].code).to.be.eq(StatusCode.BadRequest);
    expect(response.body.errors?.[0].message).to.be.eq('user.error.password.no-letter');
  });
});