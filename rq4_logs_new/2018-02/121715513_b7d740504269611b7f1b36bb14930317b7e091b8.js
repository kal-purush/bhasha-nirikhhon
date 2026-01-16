
const {ObjectID} = require('mongodb');
const {Todo}= require('./../../models/todo');
const {Users}= require('./../../models/Users');
const jwt = require('jsonwebtoken');

const userOneId = new ObjectID();

const userTwoId = new ObjectID();

const users= [{
    _id:  userOneId,
    email: 'manisham@gmail.com',
    password: 'passmanisha',
    tokens :[{
        access :'auth',
        token: jwt.sign({_id: userOneId, access : 'auth'},'abc123').toString()
    }]
},{
_id : userTwoId,
email : 'appu@gmail.com',
password: 'passmanisha'
}];

const todos = [{
    _id: new ObjectID(),
    text : 'First test todos'
  },{
    _id: new ObjectID(),
    text : 'Second test todos',
    completed: true,
    completedAt: 253455
  }];

  const populateTodos = (done)=>{
    Todo.remove({}).then(()=> {
      return Todo.insertMany(todos);
    }).then(()=>done());
};

const populateUsers= (done)=>{
    Users.remove({}).then(()=>{
      var userOne = new Users(users[0]).save();
      var userTwo  = new Users(users[1]).save();
      return Promise.all([userOne,userTwo]);
    }).then(()=>done());
};

module.exports = {todos, populateTodos,users, populateUsers};





