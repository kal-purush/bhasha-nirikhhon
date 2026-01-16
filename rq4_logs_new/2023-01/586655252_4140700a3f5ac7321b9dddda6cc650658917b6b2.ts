import { Entity, PrimaryGeneratedColumn, Column } from 'typeorm';

type UserConstructorArguments = {
  username: string;
  imageUrl?: string;
  kakaoId: string;
};

@Entity()
export class User {
  @PrimaryGeneratedColumn('increment')
  userId!: number;

  @Column({ length: 20, nullable: false })
  username: string;

  @Column()
  imageUrl?: string;

  @Column({ default: 0 })
  score!: number;

  @Column()
  kakaoId: string;

  constructor(arg: UserConstructorArguments) {
    this.username = arg.username;
    this.kakaoId = arg.kakaoId;
    this.imageUrl = arg.imageUrl;
  }
}