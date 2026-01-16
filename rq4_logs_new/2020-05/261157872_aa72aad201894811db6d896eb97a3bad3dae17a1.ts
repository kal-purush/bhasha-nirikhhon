import { expect } from 'chai';
import  requests from 'supertest';
import { getRepository, Repository } from 'typeorm';
import { User } from '../entities/user';
import { populateDB } from '../seeds';
import { hashPassword } from '../hash-password';
import * as jwt from 'jsonwebtoken';
import {ID_NOT_FOUND, AUTHEN_ERROR} from '../errors';
import {TEN_MINUTES, APP_SECRET} from '../consts';

describe('Users query tests', () => {

  let agent;
  let repository: Repository<User>;

  const usersQuery = (token, variable?: { users }) => {

    const query = `
    query ($users: Int){
      users(users: $users) {
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
    //await populateDB(100)
  })

  after( async () => {
    await repository.query('DELETE FROM user')
  })

it('should return a user', async function() {
    var token = jwt.sign({ userId: 1 }, APP_SECRET, { expiresIn: TEN_MINUTES }); 
    const res = await usersQuery(token);
    console.log(res)

//     expect(ID.toString()).to.be.equals(res.body.data.user.id);
//     expect(NAME).to.be.equals(res.body.data.user.name);
//     expect(EMAIL).to.be.equals(res.body.data.user.email);
//     expect(BDATE).to.be.equals(res.body.data.user.birthDate);
//     expect(CPF).to.be.equals(res.body.data.user.cpf);  
});

//   it('should give an error if id not found', async function() {
//     var token = jwt.sign({ userId: ID }, APP_SECRET, { expiresIn: TEN_MINUTES }); 
//     const res = await userQuery( {id: 0 }, token);
//     expect(res.body.errors[0].message).to.be.equals(ID_NOT_FOUND);
//   })

//   it('should give an error if token is expired', async function() {
//     var token = jwt.sign({ userId: ID }, APP_SECRET, { expiresIn: 0 }); 
//     const res = await userQuery( {id: ID }, token);
//     expect(res.body.errors[0].message).to.be.equals(AUTHEN_ERROR);
//   })

//   it('should give an error if token is invalid', async function() {
//     var token = "wrongtoken"
//     const res = await userQuery( {id: ID }, token);
//     expect(res.body.errors[0].message).to.be.equals(AUTHEN_ERROR);
//   })

})