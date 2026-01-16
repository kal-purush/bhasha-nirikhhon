import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  CreateDateColumn,
} from 'typeorm';
import { Card } from './card.entity';

@Entity({ name: 'card_locations' })
export class CardLocation {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ type: 'numeric', precision: 9, scale: 6 })
  latitude: number;

  @Column({ type: 'numeric', precision: 9, scale: 6 })
  longitude: number;

  @Column({ type: 'numeric', precision: 5, scale: 2, nullable: true })
  accuracy: number;

  @Column({ type: 'timestamp' })
  timestamp: Date;

  @CreateDateColumn()
  created_at: Date;

  @ManyToOne(() => Card, card => card.locations)
  card: Card;

  @Column()
  card_id: string;
} 