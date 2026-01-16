import { people } from './script.js';
import { tbody } from './element.js';
import { closeModal } from './utils.js';

 // Add new person to the list
export const addNewPeople = e => {
    e.preventDefault();
    const form = e.target;
    console.log(form);
    const newPeople = {
        id: Date.now(),
        picture: form.picture.value,
        lastName: form.lastname.value,
        firstName: form.firstname.value,
        birthday: form.birthday.value,
    }
    people.push(newPeople);
    console.log(people);
    //Reset the form
    form.reset();
    tbody.dispatchEvent(new CustomEvent('updatedTheList'));
    closeModal();
    console.log(newPeople);
}