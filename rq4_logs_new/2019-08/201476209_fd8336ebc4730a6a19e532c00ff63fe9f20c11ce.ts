import { IUser } from '@entities';

export interface IUserRepository {
    getAll(): Promise<IUser[]>;
    add(user: IUser): Promise<IUser>;
    update(user: IUser): Promise<IUser>;
    delete(id: number): Promise<IUser>;
}

export class UserInteractor {
  private userRepository: IUserRepository;

  constructor(userRepository: IUserRepository) {
    this.userRepository = userRepository;
  }

  public getAll() {
    return this.userRepository.getAll();
  }

  public add(user: IUser) {
    return this.userRepository.add(user);
  }

  public update(user: IUser) {
    return this.userRepository.update(user);
  }

  public delete(id: number) {
    return this.userRepository.delete(id);
  }
}