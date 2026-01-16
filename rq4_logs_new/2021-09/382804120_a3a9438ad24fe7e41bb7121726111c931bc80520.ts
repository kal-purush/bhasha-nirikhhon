import { useState, useEffect } from "react";

import copy from "copy-to-clipboard";

type IOptions = {
  successDuration?: number;
};

const useCopyClipboard = (
  text: string,
  options?: IOptions
): [boolean, () => void] => {
  const [isCopied, setIsCopied] = useState(false);
  const successDuration = options?.successDuration;

  useEffect(() => {
    if (isCopied && successDuration) {
      const id = setTimeout(() => {
        setIsCopied(false);
      }, successDuration);

      return () => {
        clearTimeout(id);
      };
    }
  }, [isCopied, successDuration]);

  return [
    isCopied,
    () => {
      const didCopy = copy(text);
      setIsCopied(didCopy);
    },
  ];
};

export default useCopyClipboard;