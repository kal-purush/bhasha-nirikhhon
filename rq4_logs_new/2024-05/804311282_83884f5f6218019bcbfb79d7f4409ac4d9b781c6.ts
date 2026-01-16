import {
  Entity,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  PrimaryGeneratedColumn,
  ManyToOne,
  JoinColumn,
} from 'typeorm';

import { User } from '../entities';

@Entity({ name: 'transactions' })
class Transaction {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column({
    name: 'user_id',
    type: 'uuid',
    nullable: false,
  })
  user_id!: string;

  @Column({
    name: 'from',
    type: 'varchar',
    nullable: false,
  })
  from!: string;

  @Column({
    name: 'to',
    type: 'varchar',
    nullable: false,
  })
  to!: string;

  @Column({
    name: 'block_number',
    type: 'int',
    nullable: false,
  })
  block_number!: number;

  @Column({
    name: 'block_hash',
    type: 'varchar',
    nullable: false,
  })
  block_hash!: string;

  @Column({
    name: 'transaction_hash',
    type: 'varchar',
    nullable: false,
  })
  transaction_hash!: string;

  @Column({
    name: 'gas_price',
    type: 'int',
    nullable: false,
  })
  gas_price!: number;

  @Column({
    name: 'value',
    type: 'int',
    nullable: false,
  })
  value!: number;

  @CreateDateColumn({
    name: 'created_at',
    type: 'timestamp',
    nullable: false,
  })
  created_at!: Date;

  @UpdateDateColumn({
    name: 'updated_at',
    type: 'timestamp',
    nullable: true,
  })
  updated_at!: Date;

  @ManyToOne(() => User, (user: User) => user.transactions)
  @JoinColumn({ foreignKeyConstraintName: 'id', name: 'user_id' })
  user!: User;
}

export { Transaction };