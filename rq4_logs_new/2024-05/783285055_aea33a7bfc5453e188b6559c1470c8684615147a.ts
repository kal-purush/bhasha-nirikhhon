'use client';

import { useCallback, useState } from 'react';
import '@blocknote/core/fonts/inter.css';
import '@blocknote/react/style.css';
import { usePostDetailContext } from '../../PostDetailFetcher/PostDetailContext';
import { usePostLike } from '../api/quries';

export default function useLike(postId: number) {
  const [like, setLike] = useState<boolean>(false);
  const { likeCount } = usePostDetailContext()!;
  const { mutate: postLike } = usePostLike();

  const handleLike = useCallback(() => {
    setLike((prev) => !prev);
    postLike(Number(postId));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [postId]);

  return { likeCount, like, handleLike };
}