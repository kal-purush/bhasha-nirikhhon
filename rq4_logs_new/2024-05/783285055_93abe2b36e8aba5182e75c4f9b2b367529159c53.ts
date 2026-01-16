'use client';

import { safeSeesionStorage } from '@/utils';
import { EDITOR_KEY } from '@/components/Common/Editor/constants';
import { useToastContext } from '@/components/Common/Toast/ToastProvider';
import { useCallback } from 'react';
import { usePost } from '../api/queries';
import { BlogPostRequest } from '../api';

export default function useWritePost() {
  const { handleError } = useToastContext()!;
  // TODO: blogId 사용
  const { mutate } = usePost(8);

  // TODO: refactor
  const handleSubmitPost = useCallback(
    (
      // FIXME: blogId 변경
      post: Omit<BlogPostRequest, 'content' | 'temporaryState' | 'blogId'>,
      publishState: 'publish' | 'save' = 'save',
    ) => {
      const content = safeSeesionStorage.get(EDITOR_KEY) || '';

      const { title, ...otherProps } = post;

      if (!content) {
        handleError('내용을 입력해주세요', { duration: 2000 });
        return;
      }

      if (!title) {
        handleError('제목을 입력해주세요', { duration: 2000 });
        return;
      }

      // TODO: blogId
      mutate({
        blogId: 10,
        title,
        content,
        temporaryState: publishState === 'publish',
        ...otherProps,
      });
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  );

  return { handleSubmitPost };
}