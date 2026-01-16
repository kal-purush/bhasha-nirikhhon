class person{
    constructor(firstName,lastName,salary){
        this.firstName = firstName;
        this.lastName = lastName;
        this.salary = salary;
    }
}

const heroPerson = new person("Minhazul","Abedin",700000);
console.log(heroPerson);
const friendlyPerson = new person("Suraiya","Sultana",50000);
console.log(friendlyPerson);