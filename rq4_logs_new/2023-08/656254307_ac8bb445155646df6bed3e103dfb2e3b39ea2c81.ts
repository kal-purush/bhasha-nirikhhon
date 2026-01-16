import { useEffect } from 'react';
import { EMOJI_CLASSES } from '../config/classes';
import { useElementMainRef } from '../context/RefContext';
import { EmojiType } from '../types';
import { withSelectorPrefix } from '../utils/dom-utils';

export function useActiveTypeTab(setActiveCallback: (flg: EmojiType['0']) => void) {
  const mainRef = useElementMainRef();

  useEffect(() => {
    const visited = new Map();
    const main = mainRef.current;
    let timer: ReturnType<typeof setTimeout>;

    let observer = new IntersectionObserver((entries) => {
      for (const entry of entries) {
        const id = entry.target.getAttribute('data-name');

        visited.set(id, entry.isIntersecting);
      }

      const isIntersectingList = Array.from(visited);

      for (const [id, ratio] of isIntersectingList) {
        if (ratio) {
          timer = setTimeout(() => {
            setActiveCallback(id);
          }, 100);
          break;
        }
      }
    }) as IntersectionObserver | null;

    main?.querySelectorAll(withSelectorPrefix(EMOJI_CLASSES.EmojiSection)).forEach((el) => {
      observer!.observe(el);
    });

    return () => {
      clearTimeout(timer);
      visited.clear();
      observer = null;
    };
  }, [mainRef, setActiveCallback]);
}