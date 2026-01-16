import { User } from '../users/model';
import { USERS } from '../user-storage/user-list';

export  function findUserById(id: number): User | undefined {
    return USERS.find((user: User) => user.id === id);
}
