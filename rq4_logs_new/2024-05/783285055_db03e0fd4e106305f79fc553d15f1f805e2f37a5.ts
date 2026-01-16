import { useMutation, useSuspenseQuery } from '@tanstack/react-query';
import { useToastContext } from '@/components/Common/Toast/ToastProvider';
import { getComments, postComment, postLike, getChildComments } from '.';

export const usePostLike = () => {
  const { handleSuccess } = useToastContext()!;

  return useMutation({
    mutationKey: ['post-like'],
    mutationFn: (postId: number) => postLike(postId),

    onSuccess: () => {
      handleSuccess('좋아요를 표시했어요!');
    },
  });
};

export const useComments = (postId: number, page: number) =>
  useSuspenseQuery({
    queryKey: ['get-comment', page - 1],
    queryFn: () => getComments(postId, page - 1),
    select: ({ result }) => result,
  });

export const usePostComment = (postId: number) => {
  const { handleSuccess } = useToastContext()!;

  return useMutation({
    mutationKey: ['post-comment', postId],
    mutationFn: ({
      content,
      parentId,
    }: {
      content: string;
      parentId: number;
    }) => postComment({ postId, content, parentId }),

    onSuccess: () => {
      handleSuccess('댓글을 작성했어요!');
    },
  });
};

export const useChildComments = (commentId: number, page: number) =>
  useSuspenseQuery({
    queryKey: ['get-child-comment', page - 1],
    queryFn: () => getChildComments(commentId, page - 1),
    select: ({ result }) => result,
  });