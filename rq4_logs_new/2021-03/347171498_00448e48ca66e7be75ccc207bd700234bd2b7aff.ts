import { Arg, Mutation, Resolver } from 'type-graphql';
import { Service } from 'typedi';
import { CreateUserUseCase } from '../domain/create-user.use-case';
import { UserInput } from '../domain/user.input';

@Service()
@Resolver()
export class UserResolver {
  constructor(private createUseCase: CreateUserUseCase) {}

  @Mutation(() => String, { description: 'Create user' })
  createUser(@Arg('data', () => UserInput) data: UserInput): Promise<string> {
    return this.createUseCase.exec(data);
  }
}