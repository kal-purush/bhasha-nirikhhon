import { useEffect } from 'react';

/**
 * 모달이 열렸을 때 스크롤을 방지하는 훅
 * @param isOpened 모달 열림 여부
 */
export const useDisableScroll = (isOpened: boolean) => {
  useEffect(() => {
    // 스크롤바가 사라지면서 전체 레이아웃이 이동하는 것을 방지하기 위해 스크롤바 너비만큼 패딩 추가
    const originalStyle = window.getComputedStyle(document.body);
    const originalOverflow = originalStyle.overflow;
    const originalPaddingRight = originalStyle.paddingRight;

    if (isOpened) {
      const scrollbarWidth = window.innerWidth - document.documentElement.clientWidth;

      // 스크롤바 너비만큼 패딩 추가
      document.body.style.overflow = 'hidden';
      document.body.style.paddingRight = `${scrollbarWidth / 10}rem`;
    } else {
      // 원래 상태 복구
      document.body.style.overflow = originalOverflow;
      document.body.style.paddingRight = originalPaddingRight;
    }

    return () => {
      document.body.style.overflow = originalOverflow;
      document.body.style.paddingRight = originalPaddingRight;
    };
  }, [isOpened]);
};