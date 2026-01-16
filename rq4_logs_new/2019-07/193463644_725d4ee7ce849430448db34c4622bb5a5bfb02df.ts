import { Dependencies } from '../types';
import { Repository } from 'typeorm';
import { User } from '../entities/user';
// Just a middleman currently?
export class UserService {
  repository: Repository<User>;

  constructor(options: Dependencies) {
    this.repository = options.userRepository;
  }

  find() {
    return this.repository.find();
  }

  findOne(id: number) {
    return this.repository.findOne(id);
  }

  save(user: User) {
    return this.repository.save(user);
  }

  delete(id: number) {
    return this.repository.delete(id);
  }

  // Create interface to replace generic object?
  update(id: number, obj: Object) {
    return this.repository.update(id, obj);
  }
}