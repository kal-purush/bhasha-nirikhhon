'use client';

import { generateId } from '@/utils';
import { useCallback, useState } from 'react';
import { useRecoilStickerState } from '../../store';
import { ImageProps } from '../Sticker/types';

export default function useStickerPannel() {
  const { setStickerStates } = useRecoilStickerState();
  const [showStickers, setShowStickers] = useState<boolean>(false);

  const addStickerToPanel = useCallback(
    ({
      src,
      width,
      height,
      x,
      y,
    }: Omit<ImageProps, 'resetButtonRef' | 'id'>) => {
      setStickerStates((currentImages) => [
        ...currentImages,
        {
          id: generateId(),
          width,
          height,
          x,
          y,
          src,
        },
      ]);
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  );

  return { addStickerToPanel, showStickers, setShowStickers };
}