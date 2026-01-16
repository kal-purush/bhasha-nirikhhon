import { ID, Field, InputType } from 'type-graphql';

@InputType()
export default class UpdateUserInput {
  @Field(() => ID)
  id!: string;

  @Field()
  firstName!: string;

  @Field()
  lastName!: string;

  @Field()
  email!: string;

  @Field()
  password!: string;

  // @Field(() => ID)
  // task?: Task;

  @Field({ nullable: true })
  photo?: string;

  @Field({ nullable: true })
  role?: string;
}