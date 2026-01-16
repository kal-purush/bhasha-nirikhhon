import 'reflect-metadata';
import { getRepository } from 'typeorm';
import { User } from './entities/user';
import {configServer } from './config';
import { hashPassword } from './hash-password';

var jwt = require('jsonwebtoken');

const APP_SECRET = 'PASSWORLD'; // signature
const TEN_MINUTES = 600;
const ONE_WEEK = 300000;

const { GraphQLServer } = require('graphql-yoga')

export const startServer = async () => {

  const typeDefs = `
  input LoginInputType {
    email: String!
    password: String!
    rememberMe: Boolean
  }
  type UserType {
    id: ID!
    name: String!
    email: String!
    birthDate: String!
    cpf: Int!
  }
  type LoginType {
    user: UserType!
    token: String!
  }
  type Query {
    user(id: ID!): UserType!
  }
  type Mutation {
    login(data:LoginInputType!): LoginType!
  }
  `
  
  class LoginType {
    user: User;
    token: string;
  
    constructor( user: User ,token: string ) {
      this.user = user;
      this.token = token;
    }
  }
  
  const resolvers = {
  
    Query: {
      
      async user (_, { id }){
        const user: User = await getRepository(User).findOne(id);
  
        if (user == undefined) {
          throw new Error("Invalid id.");
        }
        
        return user;
      },
    },
    
    Mutation: {
  
      login: async (_, { data } ) => {
    
        const user: User = await getRepository(User).findOne({
          where: [
            { email: data.email }
          ]
        });
        
        if (!user) {
          throw new Error("We cannot find an account with that email address");
        }
  
        if (user.password == hashPassword(data.password)) {
  
          var exp_time: number = TEN_MINUTES;
  
          if (data.rememberMe){
            exp_time = ONE_WEEK;
          };
  
          const token = jwt.sign({ userId: user.id }, APP_SECRET, {expiresIn: exp_time}); 
  
          return new LoginType(user, token);
  
        } else {
          throw new Error("Invalid credentials, please check your e-mail and password");
        };
      },
    },
  }

    configServer();

    const server = new GraphQLServer({
    typeDefs,
    resolvers,
    })

    server.start(() => console.log(`Server is running on http://localhost:4000`))
}