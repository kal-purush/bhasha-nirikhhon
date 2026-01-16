import { PostLike } from 'src/entity/post-likes.entity';
import { Post } from '../entity/post.entity';

export const likes = (subQuery) => {
  return subQuery
    .select([
      'COALESCE(COUNT(likes.postId),0) AS like_num',
      'likes.postId AS postId',
    ])
    .from(PostLike, 'likes')
    .groupBy('likes.postId');
};

export const hashtags = (subQuery) => {
  return subQuery
    .select(['posts.id AS postId', 'ARRAY_AGG(hashtags.hashtag) AS hashtags'])
    .from(Post, 'posts')
    .leftJoin('posts.hashtags', 'hashtags')
    .groupBy('posts.id');
};