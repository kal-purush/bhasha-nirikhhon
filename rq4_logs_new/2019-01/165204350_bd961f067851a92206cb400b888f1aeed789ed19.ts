import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  UpdateDateColumn,
  CreateDateColumn,
} from 'typeorm';

@Entity()
export class User {
  @PrimaryGeneratedColumn('uuid', { name: 'user_id' })
  userId: string;

  @Column({ type: 'varchar', name: 'first_name', length: 100 })
  firstName: string;

  @Column({ type: 'varchar', name: 'last_name', length: 100 })
  lastName: string;

  @Column({ type: 'varchar', name: 'mobile_phone', length: 31 })
  mobilePhone: string;

  @Column({ type: 'varchar', name: 'email', length: 200 })
  email: string;

  @Column({ type: 'varchar', name: 'password' })
  password: string;

  @CreateDateColumn()
  created: Date;

  @UpdateDateColumn()
  updated: Date;
}