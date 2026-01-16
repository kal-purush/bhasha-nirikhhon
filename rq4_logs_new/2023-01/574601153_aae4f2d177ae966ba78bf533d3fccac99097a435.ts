import {
  Column,
  CreateDateColumn,
  DeleteDateColumn,
  Entity,
  ManyToOne,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm';

import { Community } from '@domain/post/community.entity';
import { TopicProperties } from '@domain/topic/topic';

@Entity('topics')
export class Topic implements TopicProperties {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column({ unique: true })
  name!: string;

  @Column()
  url!: string;

  @Column({ nullable: true })
  promotedContent!: string | null;

  @Column()
  query!: string;

  @Column({ nullable: true })
  tweetVolume!: number | null;

  @Column()
  woeid!: string;

  @CreateDateColumn()
  createdAt!: Date;

  @UpdateDateColumn()
  updatedAt!: Date;

  @DeleteDateColumn({ nullable: true })
  deletedAt!: Date | null;

  @ManyToOne(() => Community, (community) => community)
  community!: Community;
}