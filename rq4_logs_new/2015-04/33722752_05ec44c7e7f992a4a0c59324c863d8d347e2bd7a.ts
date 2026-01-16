export default class Student{
    firstname: string;
    lastname: string;
    fullname: string;
    constructor(firstname: string, lastname: string) {
        this.firstname = firstname;
        this.lastname = lastname;
        this.fullname = (firstname + " " + lastname).trim();
    }
}