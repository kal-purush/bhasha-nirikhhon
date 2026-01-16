import {
  Column,
  CreateDateColumn,
  DeleteDateColumn,
  Entity,
  ManyToOne,
  OneToMany,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm';

import { Community } from '@domain/post/community.entity';
import { VideoChannelProperties } from '@domain/video/video';
import { Video } from '@domain/video/video.entity';

@Entity('video_channels')
export class VideoChannel implements VideoChannelProperties {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column({ unique: true })
  channelId!: string;

  @Column()
  title!: string;

  @OneToMany(() => Video, (video) => video)
  video!: Video[];

  @ManyToOne(() => Community, (community) => community)
  community!: Community;

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;

  @DeleteDateColumn({ nullable: true })
  deletedAt: Date | null;
}