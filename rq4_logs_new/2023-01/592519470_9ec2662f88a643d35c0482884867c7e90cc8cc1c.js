const { faker } = require('@faker-js/faker');

class User{
    constructor(name, username){
        this.name = name;
        this.username = username;
    }

    post(){
        let message = faker.lorem.sentence();
        let date = faker.date.recent(7)
        console.log(`____________________________________________________
        \n${this.name}  @${this.username}
        \n${message}
        \n${date}\n\n`)
    }
}

// --------- Creating Feed

console.log("FEED");

for(let i=0; i<5; i++){
    const randomName = faker.name.firstName();
    const randomUsername = faker.internet.userName(randomName);
    let user = new User(randomName, randomUsername);
    user.post();
}