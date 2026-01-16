// src/modules/user/entities/user.entity.ts
import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';
import { ObjectType, Field, ID } from '@nestjs/graphql';

@Schema()
@ObjectType()  // Decorador necessário para GraphQL
export class User extends Document {
  @Field(() => ID)
  _id: string;

  @Field()
  @Prop({ required: true })
  username: string;

  @Field()
  @Prop({ required: true })
  password: string;

  @Field()
  @Prop({ required: true })
  role: string;
}

export const UserSchema = SchemaFactory.createForClass(User);