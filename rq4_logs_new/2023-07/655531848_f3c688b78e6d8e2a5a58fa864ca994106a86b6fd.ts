import { Inject, Service } from 'typedi'

import { UserRepository } from '@database/repositories'
import { UnauthorizedException } from '@shared/exceptions'
import { Jwt } from '@utils/jwt'
import { BaseAuthMiddleware } from '@middlewares/auth/BaseAuth.middleware'

@Service()
export class AuthMiddleware extends BaseAuthMiddleware {
  constructor(
    @Inject('jsonwebtoken.jwt') protected readonly jwtService: Jwt,
    private readonly userRepository: UserRepository,
  ) {
    super()
  }

  protected async validate({ userName }: { userName: string }) {
    const user = await this.userRepository.findOneByUsername(userName)
    if (!user)
      throw new UnauthorizedException('User not found, please authenticate.')

    return user
  }
}