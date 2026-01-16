import { User } from '@application/entities/user.entity';
import { UserRepository } from '@application/repositories/user-repository';
import { HttpException, Injectable } from '@nestjs/common';

interface FindUserByIdRequest {
  userId: string;
}

interface FindUserByIdResponse {
  user: User;
}

@Injectable()
export class FindUserById {
  constructor(private userRepository: UserRepository) {}

  async execute({
    userId,
  }: FindUserByIdRequest): Promise<FindUserByIdResponse> {
    const user = await this.userRepository.findById(userId);

    if (!user) {
      throw new HttpException('O usuário não foi encontrado!', 404);
    }

    return { user };
  }
}