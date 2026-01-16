import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Hashtag } from 'src/entity/hashtag.entity';
import { Post } from 'src/entity/post.entity';
import { PostController } from './post.controller';
import { PostService } from './post.service';

@Module({
  //InjectRepository를 위해  forFeature에 entity 추가해야함
  imports: [TypeOrmModule.forFeature([Post, Hashtag])],
  controllers: [PostController],
  providers: [PostService],
  exports: [PostService],
})
export class PostModule {}