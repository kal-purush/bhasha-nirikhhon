import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  ManyToOne,
} from 'typeorm';

import User from './User';

@Entity()
export default class Expense {
  @PrimaryGeneratedColumn('uuid')
  id: number;

  @Column()
  name: string;

  @Column({
    enum: [
      'FOOD',
      'TRANSPORT',
      'ENTERTAINMENT',
      'TRAVEL',
      'HEALTH CARE',
      'UTILITIES',
      'MISCELLANEOUS',
    ],
  })
  category: string;

  @Column()
  amount: number;

  @CreateDateColumn({
    name: 'created_at',
    nullable: false,
  })
  createdAt: Date;

  @UpdateDateColumn({
    name: 'updated_at',
    nullable: false,
  })
  updatedAt: Date;

  @ManyToOne((type) => User, (user) => user.expenses)
  user: User;
}