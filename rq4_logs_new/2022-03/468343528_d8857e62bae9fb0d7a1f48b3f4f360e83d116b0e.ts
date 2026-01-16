import { UpdateUserInput } from './../../dto/update-user.input';
import { CreateUserInput } from './../../dto/create-user.input';
import { User } from '../../entities/user.entity';

const usersList: User[] = [
  new User({
    _id: '591b6e82-c353-4c14-8546-42cccabcf18c',
    name: 'Matheus Cortez',
    email: 'matheus.cortez@live.com',
    password: '1234',
    address: {
      code: '03607060',
      state: 'SP',
      city: 'São Paulo',
      district: 'Vl São Geraldo',
      address: 'Rua Guacari',
    },
  }),
  new User({
    _id: '2dbac138-0a84-4bc4-91cf-81edcc95b616',
    name: 'Tânia Bianca Caroline Pires',
    email: 'taniabiancacarolinepires.taniabiancacarolinepires@uou.com.br',
    password: 'SenhaSegura',
    address: {
      code: '44089132',
      state: 'BA',
      city: 'Feira de Santana',
      district: 'Brasília',
      address: 'Caminho 25',
    },
  }),
  new User({
    _id: 'be343819-87c9-4b42-8cc1-554c50d7fcc0',
    name: 'Laura Bianca Viana',
    email: 'laurabiancaviana-94@proimagem.com',
    password: 'SenhaSegura2',
    address: {
      code: '65911015',
      state: 'MA',
      city: 'Imperatriz',
      district: 'Parque Amazonas',
      address: 'Rua Guajara',
    },
  }),
  new User({
    _id: '16e5b096-c7d4-429c-98fc-cb724617668e',
    name: 'Renato Caio Figueiredo',
    email: 'rrenatocaiofigueiredo@dlh.de',
    password: 'SenhaSegura23',
    address: {
      code: '38411597',
      state: 'MG',
      city: 'Uberlândia',
      district: 'Shopping Park',
      address: 'Rua Felipe Bueno Campos',
    },
  }),
];

const createUserInput: CreateUserInput = {
  name: 'Matheus Silva',
  email: 'Valid@email.com',
  password: 'SenhaSecreta',
  cep: '05089001',
};

const updateUserInput: UpdateUserInput = {
  id: ' 591b6e82-c353-4c14-8546-42cccabcf18c',
  name: 'Matheus Cortez',
  cep: '05089001',
};

export default {
  giveMeOneValidUser: () => usersList[0],
  giveMeValidUsers: () => usersList,
  giveMeValidCreateInput: () => createUserInput,
  giveMeValidUpdateInput: () => updateUserInput,
};