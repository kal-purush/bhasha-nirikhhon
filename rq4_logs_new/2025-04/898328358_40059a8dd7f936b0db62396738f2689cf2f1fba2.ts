"use client";

import { useEffect, useRef, useState } from "react";

const useHeaderBar = () => {
  const [showHeader, setShowHeader] = useState(false);
  const prevScrollY = useRef(0);

  // スクロールヘッダーのコード
  useEffect(() => {
    // 初期スクロール位置を設定（ページリロード時にヘッダーが表示されないように）
    const currentScrollY = window.scrollY;
    if (currentScrollY < 100) {
      setShowHeader(false); // スクロール位置が100px未満の場合、ヘッダーを隠す
    }

    const handleScroll = () => {
      const currentScrollY = window.scrollY;

      //
      if (currentScrollY > 100) {
        setShowHeader(true); // 下にスクロールしたらヘッダーを表示
      } else if (currentScrollY < 100) {
        setShowHeader(false); // 上にスクロールしたらヘッダーを隠す
      }

      prevScrollY.current = currentScrollY; // 前回のスクロール位置を更新
    };

    window.addEventListener("scroll", handleScroll);
    return () => window.removeEventListener("scroll", handleScroll);
  }, []); // prevScrollY を useRef で管理しているので、依存配列は空にする
  return {
    showHeader,
  };
};

export default useHeaderBar;