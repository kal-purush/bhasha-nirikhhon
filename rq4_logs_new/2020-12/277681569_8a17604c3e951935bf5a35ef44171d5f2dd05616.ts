import {
  Entity, Column, PrimaryColumn, CreateDateColumn,
} from 'typeorm';
import { StreamSummaryTest2 } from '@truepoint/shared/dist/interfaces/StreamSummaryTest2.interface';

@Entity({ name: 'StreamSummaryTest2' })
export class StreamSummaryTest2Entity implements StreamSummaryTest2 {
  @PrimaryColumn()
  streamId: string;

  @PrimaryColumn()
  platform: string;

  @Column()
  creatorId: string;

  @Column()
  smileCount: number;

  @CreateDateColumn()
  createdAt: Date;

  constructor(partial: Partial<StreamSummaryTest2Entity>) {
    Object.assign(this, partial);
  }
}