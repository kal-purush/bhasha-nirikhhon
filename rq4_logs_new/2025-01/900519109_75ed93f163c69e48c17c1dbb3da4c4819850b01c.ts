import { RefObject, useEffect, useState } from 'react';

interface Props {
  ref: RefObject<HTMLParagraphElement | null>;
  description: string;
}

export const useIsMoreButtonVisible = ({ ref, description }: Props) => {
  const [isMoreButtonVisible, setIsMoreButtonVisible] = useState(false);

  useEffect(() => {
    if (ref.current) {
      const element = ref.current;
      const lineHeight = parseFloat(window.getComputedStyle(element).lineHeight || '1.5');
      const maxHeight = lineHeight * 6; // 6줄 기준 높이 계산

      if (element.scrollHeight > maxHeight) {
        setIsMoreButtonVisible(true);
      }
    }
  }, [ref, description]);

  return isMoreButtonVisible;
};