const {ObjectID} = require('mongodb');

const {mongoose} = require('./../server/db/mongoose');
const {Todo} = require('./../server/models/todo');
const {User} = require('./../server/models/user');

// var id = '59488e3eee0c1a1e09f77520';
var userID = '5941f921001ca41704bd3107';

// if(!ObjectID.isValid(id)){
//     return console.log('Object ID is not valid.');
// }

if(!ObjectID.isValid(userID)){
    return console.log('Object ID is not valid.');
}

// Todo.find({
//     _id: id
// }).then((todos) => {
//     console.log('Todos: ', todos);
// });

// Todo.findOne({
//     _id: id
// }).then((todo) => {
//     console.log('Todo: ', todo);
// });

// Todo.findById(id).then((todo) => {
//     console.log('Todo by ID:', todo)
// }).catch((e) => {
//     console.log(e);
// });

//User
//1. User not found
//2. User was found. Print User
//3. Print error
User.findById(userID).then((user) => {
    if (!user) {
        return console.log('User not found.');
    }
    else {
        console.log('User: ', user);
    }
}).catch((e) => {
    console.log(e);
});