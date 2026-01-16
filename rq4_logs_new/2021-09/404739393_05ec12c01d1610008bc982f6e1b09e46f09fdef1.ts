import {
  Column,
  Entity,
  JoinColumn,
  ManyToOne,
  OneToMany,
  PrimaryGeneratedColumn,
} from 'typeorm';
import { User } from '../../user/entity/user.entity';

@Entity('channel')
export class Channel {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ nullable: false })
  title: string;

  @Column({ nullable: false })
  type: number;

  @Column({ nullable: true })
  password: string;

  @OneToMany(() => ChannelMember, (channelMember) => channelMember.channelID)
  channelIDs: ChannelMember[];
}

@Entity('channel_member')
export class ChannelMember {
  @ManyToOne(() => Channel, (channel) => channel.channelIDs, { primary: true })
  @JoinColumn({ name: 'channelID' })
  channelID: Channel;

  @ManyToOne(() => User, { primary: true })
  @JoinColumn({ name: 'userID' })
  userID: User;

  @OneToMany(() => ChannelMessage, (message) => message.sentBy)
  messages: ChannelMessage[];

  @Column()
  permissionType: number;

  @Column()
  penalty: number;
}

@Entity('channel_message')
export class ChannelMessage {
  @PrimaryGeneratedColumn()
  id: number;

  @ManyToOne(() => ChannelMember, (message) => message.messages)
  @JoinColumn([
    { name: 'cid', referencedColumnName: 'channelID' },
    { name: 'uid', referencedColumnName: 'userID' },
  ])
  sentBy: ChannelMember;

  @Column()
  message: string;

  @Column()
  timestamp: Date;
}