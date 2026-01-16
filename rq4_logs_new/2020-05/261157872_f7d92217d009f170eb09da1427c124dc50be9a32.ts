import * as faker from 'faker';
import { getRepository, createConnection, Connection } from 'typeorm';
import { User } from './entities/user'
import { hashPassword} from './hash-password'
import * as dotenv from 'dotenv';
import path from 'path';

const USERS: number = 10;

const populateDB = async () => {

    dotenv.config({ path: path.join(__dirname, '..', ".env.test") });
    await createConnection();

    const repository = getRepository(User)

    for (let i = 0; i < USERS; i++) {

        const newUser: User = new User();

        newUser.name = faker.name.findName();
        newUser.email = faker.internet.email();
        newUser.birthDate = faker.date.past();
        newUser.cpf = faker.random.number();
        newUser.password = hashPassword('password1');

        repository.save(newUser)
        console.log("Added user " + newUser.name)
    }
}

populateDB();