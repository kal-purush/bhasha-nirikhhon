import { useState } from 'react';

type CopiedValue = string | null;
type CopyFn = (text: string) => Promise<boolean>; // Return success

/**
 * Copies a block of code to the clipboard.
 *
 * @source https://usehooks-ts.com/react-hook/use-copy-to-clipboard
 * @returns [CopiedValue, CopyFn]
 */
export const useCopyToClipboard = (): [boolean, CopyFn] => {
  const [, setCopiedText] = useState<CopiedValue>(null);
  const [isCopied, setIsCopied] = useState<boolean>(false);

  const copy: CopyFn = async (text) => {
    if (!navigator?.clipboard) {
      console.warn('Clipboard not supported');
      return false;
    }

    // Try to save to clipboard then save it in the state if worked
    try {
      await navigator.clipboard.writeText(text);
      setCopiedText(text);
      setIsCopied(true);

      setTimeout(() => {
        setIsCopied(false);
      }, 3000);

      return true;
    } catch (error) {
      console.warn('Copy failed', error);
      setCopiedText(null);
      setIsCopied(false);
      return false;
    }
  };

  return [isCopied, copy];
};