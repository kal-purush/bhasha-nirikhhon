/* eslint-disable no-sync */
const bcrypt = require('bcrypt');

const { saltRounds } = require('../resources/users/user.service');

const users = [
  {
    name: 'Admin',
    login: 'admin',
    password: 'admin'
  }
].map(user => ({
  ...user,
  password: bcrypt.hashSync(user.password, saltRounds)
}));

const boards = [
  {
    id: 'id1',
    title: 'Sprint1',
    columns: [{ title: 'column1', order: 1 }]
  },
  {
    id: 'id2',
    title: 'Sprint2',
    columns: [{ title: 'column1', order: 1 }]
  }
];

const tasks = [
  {
    title: 'Read book',
    order: 1,
    description: 'Read book'
  },
  {
    title: 'Do exersices',
    order: 2,
    description: 'Do exersices'
  }
];

module.exports = { users, boards, tasks };