import { useState } from "react";

/**
 * Hook para copiar texto al portapapeles.
 * @param text El texto a copiar.
 * @returns El estado de la acción de copiar (si fue copiado o no).
 */
export const useCopyToClipboard = (text: string) => {
  const [copied, setCopied] = useState(false);

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error("Error al copiar el texto:", err);
    }
  };

  return { copied, copy };
};