import { expect } from 'chai'
import  requests from 'supertest';
import { getRepository, Repository } from 'typeorm';
import {User} from '../entities/user'
import { hashPassword } from '../hash-password';
import * as jwt from 'jsonwebtoken';
import {ID_NOT_FOUND, AUTHEN_ERROR} from '../errors';
import {TEN_MINUTES, APP_SECRET} from '../consts';

var ID = 1
const NAME = "Hugo"
const EMAIL = "hugo@gmail.com"
const EMAIL2 = "joao@gmail.com"
const PASSW = "password1"
const CPF = "12345678900"
const BDATE = "04/06/1993"

describe('User query tests', () => {

  let agent;
  let repository: Repository<User>;

  const userMutation = (variable: {id}, token) => {

    const query = `
    query ($id: ID!){
      user(id: $id) {
        id 
        name 
        email 
        birthDate 
        cpf
      }
    }
    `
    return agent.post("/").send({ query: query, variables: variable }).set('Authorization', token)
  }

  before(async () => {
    agent = requests('http://localhost:4000');
    repository = getRepository(User);

    const user = new User();

    user.name = NAME;
    user.email = EMAIL;
    user.birthDate = BDATE;
    user.cpf = CPF;
    user.password = hashPassword(PASSW);

    await repository.save(user);
    ID = (await repository.findOne({email: EMAIL})).id
  })

  after( async () => {
    await repository.delete({ email: EMAIL})
    await repository.delete({ email: EMAIL2})
  })

  it('should return a user', async function() {
    var token = jwt.sign({ userId: 1 }, APP_SECRET, { expiresIn: TEN_MINUTES }); 
    const res = await userMutation( {id: ID }, token);
    expect(ID.toString()).to.be.equals(res.body.data.user.id);
    expect(NAME).to.be.equals(res.body.data.user.name);
    expect(EMAIL).to.be.equals(res.body.data.user.email);
    expect(BDATE).to.be.equals(res.body.data.user.birthDate);
    expect(CPF).to.be.equals(res.body.data.user.cpf);  
  });

  it('should giva an error if id not found', async function() {
    var token = jwt.sign({ userId: 1 }, APP_SECRET, { expiresIn: TEN_MINUTES }); 
    const res = await userMutation( {id: 0 }, token);
    expect(res.body.errors[0].message).to.be.equals(ID_NOT_FOUND);
  })

  it('should give an error if token is expired', async function() {
    var token = jwt.sign({ userId: 1 }, APP_SECRET, { expiresIn: 0 }); 
    const res = await userMutation( {id: 0 }, token);
    expect(res.body.errors[0].message).to.be.equals(AUTHEN_ERROR);
  })

  it('should give an error if token is invalid', async function() {
    var token = "wrongtoken"
    const res = await userMutation( {id: 0 }, token);
    expect(res.body.errors[0].message).to.be.equals(AUTHEN_ERROR);
  })

})