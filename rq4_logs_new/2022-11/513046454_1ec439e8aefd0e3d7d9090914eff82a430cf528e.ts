import { Entity, BaseEntity, ManyToOne, PrimaryColumn, Column } from "typeorm";
import { Field, ObjectType } from "type-graphql";
import { User } from "./User";
import { Post } from "./Post";

@ObjectType()
@Entity()
export class Updoot extends BaseEntity {
  @Column({ type: 'int' })
  value: number;

  @Field()
  @PrimaryColumn()
  userId!: number;

  @Field(() => User)
  @ManyToOne(() => User, (user) => user.updoots)
  user: User

  @PrimaryColumn()
  postId!: number;

  @Field(() => User)
  @ManyToOne(() => Post, (post) => post.updoots)
  post: Post
}