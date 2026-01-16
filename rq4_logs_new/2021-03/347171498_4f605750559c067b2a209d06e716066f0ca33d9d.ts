import { Field, InputType } from 'type-graphql';
import { UserInputModel } from '@domain/model/user.model';
import { IsEmail, Validate } from 'class-validator';
import { PasswordValidator } from './password.validator';

@InputType()
export class CreateUserInput implements UserInputModel {
  @Field({ description: 'User name' })
  name!: string;

  @IsEmail(undefined, { message: 'invalid email' })
  @Field({ description: 'User email' })
  email!: string;

  @Validate(PasswordValidator)
  @Field({ description: 'User password' })
  password!: string;
}