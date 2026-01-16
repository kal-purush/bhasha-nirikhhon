import { PostComment } from 'src/entity/post-comments.entity';
import { PostLike } from 'src/entity/post-likes.entity';
import { User } from 'src/entity/user.entity';
import {
  Column,
  Entity,
  JoinTable,
  ManyToMany,
  ManyToOne,
  OneToMany,
  PrimaryColumn,
} from 'typeorm';
import { Hashtag } from './hashtag.entity';
import { PostImage } from './post-images.entity';

@Entity('posts')
export class Post {
  @PrimaryColumn()
  id: number;

  @Column()
  title: string;

  @Column()
  content: string;

  @Column()
  hits: number;

  @ManyToOne(() => User, (user) => user.posts)
  user: User;

  @OneToMany(() => PostComment, (postComment) => postComment.post)
  postComments: PostComment[];

  @OneToMany(() => PostLike, (postLike) => postLike.post)
  postLikes: PostLike[];

  @OneToMany(() => PostImage, (postImage) => postImage.post)
  postImages: PostImage[];

  @ManyToMany(() => Hashtag)
  @JoinTable()
  hashtags: Hashtag[];
}