import { NumbericBoolean } from 'src/type';
import {
  Column,
  Entity,
  PrimaryGeneratedColumn,
  CreateDateColumn,
} from 'typeorm';

@Entity('Payment_Option')
export class PaymentOption {
  @PrimaryGeneratedColumn()
  option_id: number;

  @Column({ type: 'varchar', length: 64, nullable: false, unique: false })
  name: string;

  @Column({ type: 'tinyint', nullable: false, unique: false })
  is_active: NumbericBoolean;

  @CreateDateColumn({
    type: 'datetime',
    nullable: false,
    unique: false,
    default: () => 'CURRENT_TIMESTAMP',
  })
  public created_at: Date;
}