import { Bank } from '@modules/bank/typeorm/entities/Bank';
import { User } from '@modules/users/typeorm/entities/User';
import {
  Column,
  CreateDateColumn,
  DeleteDateColumn,
  Entity,
  JoinColumn,
  ManyToOne,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity('transactions')
export class Transaction {
  @PrimaryGeneratedColumn('increment')
  id: number;

  @JoinColumn({ name: 'user_id' })
  @ManyToOne(() => User, user => user.transactions)
  user: User;

  @JoinColumn({ name: 'bank_id' })
  @ManyToOne(() => Bank, bank => bank.transactions)
  bank: Bank;

  @Column({ name: 'user_id' })
  userId: number;

  @Column({ name: 'bank_id' })
  bankId: number;

  @Column({ name: 'old_balance' })
  oldBalance: number;

  @Column()
  type: string; //enum

  @Column()
  price: number;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;

  @DeleteDateColumn({ name: 'deleted_at' })
  deletedAt: Date;
}