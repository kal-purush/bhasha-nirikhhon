import { Entity, Column, PrimaryGeneratedColumn, CreateDateColumn, UpdateDateColumn } from 'typeorm';

export enum ReferralStatus {
  PENDING = 'Pending',
  QUOTE_PROVIDED = 'Quote Provided',
  INSTALLATION_SCHEDULED = 'Installation Scheduled',
  COMPLETED = 'Completed',
}

export enum RewardStatus {
  PENDING = 'Pending',
  PAID = 'Paid',
}

@Entity()
export class Referral {
  @PrimaryGeneratedColumn()
  id: number;

  @Column()
  referrer: string;

  @Column()
  referrerLocation: string;

  @Column({ unique: true })
  referralCode: string;

  @Column()
  referredCustomer: string;

  @Column()
  referredLocation: string;

  @Column({
    type: 'enum',
    enum: ReferralStatus,
    default: ReferralStatus.PENDING,
  })
  status: ReferralStatus;

  @Column({
    type: 'enum',
    enum: RewardStatus,
    default: RewardStatus.PENDING,
  })
  rewardStatus: RewardStatus;

  @Column({ default: '$50' })
  amount: string;

  @Column({ nullable: true })
  email: string;

  @Column({ nullable: true })
  phone: string;

  @Column({ type: 'text', nullable: true })
  notes: string;

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
} 