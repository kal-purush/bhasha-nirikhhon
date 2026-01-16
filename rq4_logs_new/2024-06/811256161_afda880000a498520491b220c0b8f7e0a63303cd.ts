import { useEffect, useState } from 'react';

const useAutosizeTextArea = (textAreaRef: HTMLTextAreaElement | null, value: string | null) => {
  const [initialHeight, setInitialHeight] = useState<number>(0);

  useEffect(() => {
    if (textAreaRef) {
      if (!initialHeight) {
        const scrollHeight = textAreaRef.scrollHeight;

        if (scrollHeight) setInitialHeight(scrollHeight);
      } else {
        textAreaRef.style.height = initialHeight + 'px';

        const scrollHeight = textAreaRef.scrollHeight;

        textAreaRef.style.height = scrollHeight + 'px';
      }
    }
  }, [textAreaRef, value, initialHeight]);
};

export default useAutosizeTextArea;