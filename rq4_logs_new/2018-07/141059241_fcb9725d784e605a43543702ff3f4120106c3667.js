const {ObjectID} = require('mongodb');

const {mongoose} = require('./../server/db/mongoose');


const {Todo} = require('./../server/models/todo');
const {User} = require('./../server/models/user');


// var id = '5b4bc8a63ab0cc8495a06c9a';
//
// if (!ObjectID.isValid(id)) {
//   console.log('ID not valid');
// }
//
// // Todo.find({
// //   _id: id
// // }).then((todos) => {
// //   console.log('Todos', todos);
// // });
// //
// // Todo.findOne({
// //   _id: id
// // }).then((todo) => {
// //   console.log('Todo', todo);
// //   });
//
// Todo.findById(id).then((todo) => {
//   if (!todo) {
//     return console.log('File not found');
//   }
//   console.log('Todo by id', todo);
// }).catch((e) => console.log(e));


User.findById('5b4bc8a63ab0cc8495a06c9a').then((user) => {
  if(!user) {
    return console.log('User not found');
  }
  console.log('User by id', user);
}, (e) => { console.log(e);
});