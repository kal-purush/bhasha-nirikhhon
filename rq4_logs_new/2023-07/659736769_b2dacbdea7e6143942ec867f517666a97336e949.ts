import { User } from '@application/entities/user.entity';
import { UserRepository } from '@application/repositories/user-repository';
import { HttpException, Injectable } from '@nestjs/common';

import * as bcrypt from 'bcrypt';

interface UpdateUserByIdRequest {
  user_uuid: string;
  name?: string;
  email?: string;
  password?: string;
  avatar?: string;
}

@Injectable()
export class UpdateUserById {
  constructor(private userRepository: UserRepository) {}

  async execute({
    user_uuid,
    email,
    name,
    password,
    avatar,
  }: UpdateUserByIdRequest): Promise<any> {
    const currentUser = await this.userRepository.findById(user_uuid);

    if (!currentUser) {
      throw new HttpException('Algo deu errado!', 404);
    }

    if (email && (await this.userRepository.checkIfUserExistsByEmail)) {
      throw new HttpException('Este email já está sendo utilizado!', 409);
    }

    const saltOrRounds = 8;
    const checkIfNewPasswordExists = password
      ? await bcrypt.hash(password, saltOrRounds)
      : currentUser.password;

    const newUpdatedDate = new Date();

    const updatedUser = new User(
      {
        email: email ?? currentUser.email,
        name: name ?? currentUser.name,
        password: checkIfNewPasswordExists,
        avatar: avatar ?? currentUser.avatar,
        updatedAt: newUpdatedDate,
        createdAt: currentUser.createdAt,
      },
      user_uuid,
    );

    await this.userRepository.update(updatedUser);
  }
}