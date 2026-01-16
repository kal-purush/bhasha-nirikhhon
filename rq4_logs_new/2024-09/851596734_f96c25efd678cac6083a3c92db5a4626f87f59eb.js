import { faker } from '@faker-js/faker';

export default class RandomUser {
    constructor() {
        this.username = faker.internet.userName();
        this.email = faker.internet.email();
        this.password = faker.internet.password();
    }

    get getUsername() {
        return this.username;
    }
    get getEmail() {
        return this.email;
    }
    get getPassword() {
        return this.password;
    }
}