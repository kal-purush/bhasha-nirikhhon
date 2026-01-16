import { User } from './models/User';

const user2 = new User({ age: 14 });
user2.set({ name: 'Javad Agha' });
console.log('user 2 >> name:', user2.get('name'));
console.log('user 2 >> age:', user2.get('age'));

const user = new User({ name: 'Javad', age: 35 });
user.set({ name: 'Javad Agha' });
user.set({ name: 'Javad Malek', age: 25 });
console.log('user >> name:', user.get('name'));
console.log('user >> age: ', user.get('age'));

user.on('click', () => {
  console.log('on click #1');
});
user.on('click', () => {
  console.log('on click #2');
});
user.on('click', () => {
  console.log('on click #3');
});
user.on('mouseover', () => {
  console.log('on mouseover #1');
});
user.on('mouseover', () => {
  console.log('on mouseover #2');
});

console.log(user);
user.trigger('click');
user.trigger('mouseover');
user.trigger('skdjfhgksjdf');