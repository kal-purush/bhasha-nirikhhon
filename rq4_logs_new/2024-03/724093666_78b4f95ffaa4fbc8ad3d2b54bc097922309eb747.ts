import { Column, Entity, PrimaryGeneratedColumn } from 'typeorm';

@Entity('Manual_Cutoff_Time')
export class ManualCutoffTime {
  @PrimaryGeneratedColumn()
  public manual_id: number;

  @Column({ type: 'date', nullable: false, unique: false })
  public date: Date;

  @Column({ type: 'int', nullable: false, unique: false })
  public cutoff_time_m: number;

  @Column({ type: 'int', nullable: false, unique: false })
  public restaurant_id: number;

  @Column({ type: 'bigint', nullable: false, unique: false })
  public logged_at: number;
}