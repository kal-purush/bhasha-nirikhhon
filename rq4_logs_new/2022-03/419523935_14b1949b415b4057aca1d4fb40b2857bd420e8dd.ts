import User from '../infra/typeorm/entities/User';
import IUserRepository from '../repositories/IUserRepository';
import { inject, injectable } from 'tsyringe';

@injectable()
export default class FindByIdService {
  constructor(
    @inject('UserRepository')
    private userRepository: IUserRepository,
  ) {}

  public async execute(userId: string): Promise<User>{
    let userAtDatabase = await this.userRepository.findById(userId);
    
    if (!userAtDatabase){
      throw new Error('User not found');
    }
    
    return userAtDatabase!;
  }
}