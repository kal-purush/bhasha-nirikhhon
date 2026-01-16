import { CategoryEntity } from './categories.entity';
import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  BaseEntity,
  CreateDateColumn,
  UpdateDateColumn,
  ManyToMany,
  JoinTable,
  DeleteDateColumn,
  ManyToOne,
  JoinColumn,
} from 'typeorm';
import { TagEntity } from './tags.entity';

@Entity('posts')
export class PostEntity extends BaseEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column('varchar', { length: 255 })
  title: string;

  @Column('text')
  content: string;

  @Column('int')
  user_id: number;

  @Column('boolean', { default: false })
  public_status: boolean;

  @CreateDateColumn({ type: 'timestamptz' })
  created_at: Date;

  @UpdateDateColumn({ type: 'timestamptz' })
  updated_at: Date;

  @DeleteDateColumn({ type: 'timestamptz' })
  deleted_at?: Date | null;

  @ManyToOne(
    () => CategoryEntity,
    (category_id: CategoryEntity) => category_id.posts,
  )
  @JoinColumn([
    {
      name: 'category_id',
      referencedColumnName: 'id',
    },
  ])
  category_id: CategoryEntity;

  @ManyToMany(() => TagEntity, (tag: TagEntity) => tag.posts, {
    cascade: true,
  })
  @JoinTable({
    name: 'posts_tags',
    joinColumn: {
      name: 'post_id',
      referencedColumnName: 'id',
    },
    inverseJoinColumn: {
      name: 'tag_id',
      referencedColumnName: 'id',
    },
  })
  tags: TagEntity[];
}