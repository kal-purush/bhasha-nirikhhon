import { UserDTO } from '../types/user';
import {
  ForumCommentExtended,
  Forum,
  PostExtended,
  ThreadDislikeDTO,
  ThreadExtended,
  ThreadLikeDTO,
  ForumThreadTransformed,
  ThreadDTO,
  PostDTO,
  ForumCommentDTO,
  ForumCommentTransformed,
  ForumEntityTransformed,
} from '../types/forum';

export function transformForumData(data: Forum): ForumThreadTransformed[] {
  const { threads, posts, comments } = data;
  const forum = threads.map((thread: ThreadExtended) => {
    const threadLikes = thread.likes.map(
      (threadLike: ThreadLikeDTO) => threadLike.userId
    );

    const threadDislikes = thread.dislikes.map(
      (threadDislike: ThreadDislikeDTO) => threadDislike.userId
    );
    const threadPosts = posts.filter(
      (post: PostExtended) => post.threadId === thread.threadId
    );
    const postComments = comments.filter((comment: ForumCommentExtended) =>
      threadPosts.some(
        (post: PostExtended) => Number(post.postId) === Number(comment.postId)
      )
    );

    return {
      threadId: thread.threadId,
      author: thread.user.name,
      avatar: thread.user.avatar,
      content: thread.content,
      datetime: thread.createdAt,
      likes: threadLikes,
      dislikes: threadDislikes,
      comments: threadPosts.map(post => ({
        postId: post.postId,
        threadId: thread.threadId,
        author: post.user.name,
        avatar: post.user.avatar,
        content: post.content,
        likes: post.likes.map(postLike => postLike.userId),
        dislikes: post.dislikes.map(postDislike => postDislike.userId),
        datetime: post.createdAt,
        comments: postComments.reduce(
          (acc: ForumCommentTransformed[], comment: ForumCommentExtended) => {
            if (Number(comment.postId) === Number(post.postId)) {
              acc.push({
                commentId: Number(comment.commentId),
                postId: Number(post.postId),
                author: comment.user.name,
                avatar: comment.user.avatar,
                content: String(comment.content),
                datetime: String(comment.createdAt),
                likes: comment.likes.map(commentLike => commentLike.userId),
                dislikes: comment.dislikes.map(
                  commentDislike => commentDislike.userId
                ),
              });
            }
            return acc;
          },
          []
        ),
      })),
    };
  });

  return forum as unknown as ForumThreadTransformed[];
}

export function transformForumDTOtoStore(
  type: string,
  dto: ThreadDTO | PostDTO | ForumCommentDTO,
  user: UserDTO
): ForumEntityTransformed {
  return {
    [`${type}Id`]: dto[Object.keys(dto)[0]],
    author: user.first_name,
    avatar: user.avatar,
    content: String(dto.content),
    datetime: String(dto.createdAt),
    likes: [],
    dislikes: [],
    comments: [],
  } as unknown as ForumEntityTransformed;
}

const forumUtils = {
  transformForumData,
  transformForumDTOtoStore,
};

export default forumUtils;