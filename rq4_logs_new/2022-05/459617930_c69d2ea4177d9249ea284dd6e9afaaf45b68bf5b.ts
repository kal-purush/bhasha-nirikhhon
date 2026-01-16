import { useState, useEffect, useCallback } from 'react';

export type TUseCurrentTyping = {
  currentIndex: number;
  isCollectType: boolean;
};

export const useCurrentTyping = (
  text: string,
  onClearDisplayWord: () => void,
  countUpMissTypes: () => void
): TUseCurrentTyping => {
  // クリア文字数カウント
  const [currentIndex, setCurrentIndex] = useState(0);
  // 正しい文字をタイプしたかのフラグ
  const [isCollectType, setIsCollectType] = useState(true);

  useEffect(() => {
    if (!isCollectType) countUpMissTypes();
  }, [isCollectType]);

  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      // タイプミスフラグの初期化
      setIsCollectType(true);

      // 現在のタイピング対象の文字
      const currentChar = text[currentIndex];
      // 次にタイピング対象になる文字
      const nextChar = text[currentIndex + 1];

      if (!currentChar) return;
      // Shiftキーが入力された場合はリターン
      if (e.key === 'Shift') return;
      if (e.key !== currentChar) {
        // 入力失敗の場合 isCollectType を　 false にしてリターン
        setIsCollectType(false);
        return;
      }

      setCurrentIndex(currentIndex + 1);
      // 次の文字が空白だった場合、currentIndexのカウントをさらに1進める
      if (nextChar === ' ') setCurrentIndex(currentIndex + 2);
    },
    [text, currentIndex, setCurrentIndex]
  );

  useEffect(() => {
    document.addEventListener('keydown', handleKeyDown);

    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [handleKeyDown]);

  useEffect(() => {
    if (text.length === currentIndex) {
      // clear
      onClearDisplayWord();
      // クリア文字数カウントを0に戻す
      setCurrentIndex(0);
    }
  }, [currentIndex]);

  return { currentIndex, isCollectType };
};