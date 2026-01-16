import { createHash } from "crypto";
import { UserEntity } from "src/modules/users/entities/user.entity";

export class FakeDatabase {
    public static readonly users: UserEntity[] = [
        {
            id: '1',
            name: 'admin',
            password: createHash('sha256').update('test').digest('hex'),
            email: 'admin@gmail.com',
        },
        {
            id: '2',
            name: 'noxfly',
            password: createHash('sha256').update('123').digest('hex'),
            email: 'nox.fly@gmail.com',
        }
    ];
}