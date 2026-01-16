import { UserEntity } from './users.entity';
import {
  BaseEntity,
  Column,
  Entity,
  JoinColumn,
  ManyToOne,
  OneToMany,
  PrimaryGeneratedColumn,
} from 'typeorm';
import { TagEntity } from './tags.entity';

@Entity('tag_folders')
export class TagFolderEntity extends BaseEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column('varchar', { length: 30 })
  name: string;

  @OneToMany(() => TagEntity, (tag: TagEntity) => tag.folder_id, {
    cascade: true,
  })
  tags: TagEntity[];

  @ManyToOne(() => UserEntity, (user_id: UserEntity) => user_id.tag_folders)
  @JoinColumn([
    {
      name: 'user_id',
      referencedColumnName: 'id',
    },
  ])
  user_id: UserEntity;
}