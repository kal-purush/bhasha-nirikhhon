import { Entity, PrimaryColumn, CreateDateColumn, Column } from 'typeorm';

@Entity('Order_Status')
export class OrderStatusEntity {
  @PrimaryColumn()
  public order_status_id: string;

  @Column({ type: 'tinyint', nullable: false, unique: false })
  public is_active: number;

  @Column({ type: 'tinyint', nullable: true, unique: false })
  public is_end_status: number;

  @CreateDateColumn({
    type: 'datetime',
    nullable: false,
    unique: false,
    default: () => 'CURRENT_TIMESTAMP',
  })
  created_at: Date;
}