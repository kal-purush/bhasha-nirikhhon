import { UseGuards } from '@nestjs/common';
import { Query, Resolver } from '@nestjs/graphql';
import { CodeheroesAuthService as AuthService } from './auth.service';
import { CtxUser } from './decorators/ctx-user.decorator';
import { GqlAuthGuard } from './guards/gql-auth.guard';
import { AuthUser } from './models/user';

@Resolver()
export class AuthResolver {
  constructor(private readonly authService: AuthService) {}

  @UseGuards(GqlAuthGuard)
  @Query(() => AuthUser, { name: 'me' })
  me(@CtxUser() user: AuthUser) {
    return user;
  }

}