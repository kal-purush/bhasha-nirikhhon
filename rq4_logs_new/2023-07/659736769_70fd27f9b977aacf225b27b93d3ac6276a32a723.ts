import { User } from '@application/entities/user.entity';
import { UserRepository } from '@application/repositories/user-repository';
import { Injectable } from '@nestjs/common';
import * as bcrypt from 'bcrypt';

interface UserAuthRequest {
  email: string;
  password: string;
}

interface UserAuthResponse {
  user: User | null;
}

@Injectable()
export class UserAuth {
  constructor(private userRepository: UserRepository) {}

  async execute(email: string, password: string): Promise<any> {
    const userExists = await this.userRepository.checkIfUserExistsByEmail(
      email,
    );

    if (userExists) {
      const comparedPassword = await bcrypt.compare(
        password,
        userExists.password,
      );

      if (comparedPassword) {
        return userExists;
      }
    }

    return null;
  }
}