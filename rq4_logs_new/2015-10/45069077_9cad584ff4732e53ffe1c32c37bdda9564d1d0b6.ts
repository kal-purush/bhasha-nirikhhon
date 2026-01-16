import { Component, CORE_DIRECTIVES, FORM_DIRECTIVES } from 'angular2/angular2';

import { NameList } from '../../services/name_list';

class User {
  firstName: string;
  lastName: string;
  age: number;

  constructor (first, last, age) {
    this.firstName = first;
    this.lastName = last;
    this.age = age;
  }

  greet () {
    return 'Hello, I\'m ' + this.firstName + ' ' + this.lastName + ' and I\'m ' + this.age;
  }
}



@Component({
  selector: 'about',
  templateUrl: './components/about/about.html',
  directives: [CORE_DIRECTIVES, FORM_DIRECTIVES]
})
export class AboutCmp {

  salutation: string;
  users: Array<User>;
  firstName: string;
  lastName: string;
  age: number;

  constructor(public list: NameList) {
    this.users = [];
  }

  addName(newname): boolean {
    this.list.add(newname.value);
    newname.value = '';
    // prevent default form submit behavior to refresh the page
    return false;
  }

  createUser (first, last, age) {
    var newUser = new User(first, last, age);
    this.users.push(newUser);
    this.salutation = newUser.greet();

    this.firstName = '';
    this.lastName = '';
    this.age = null;
  }



}