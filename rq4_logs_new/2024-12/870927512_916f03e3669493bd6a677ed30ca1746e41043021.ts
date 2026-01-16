import { useCallback, useEffect, useRef, useState } from 'react';
import type { ChatLogType } from '../types';

type UseAutoScrollArg = {
  chatLog: ChatLogType;
};

export const useAutoScroll = ({ chatLog }: UseAutoScrollArg) => {
  const [canScrollBottom, setCanScrollBottom] = useState<boolean>(true);

  const scrollRouteRef = useRef<HTMLDivElement | null>(null);

  /**
   * スクロール位置を一番下まで移動させます。
   *
   * この関数は、チャットログのコンテナ要素が存在する場合に、そのスクロール位置を
   * コンテナのスクロール高さまで移動させることで、最新のメッセージが表示されるようにします。
   */
  const scrollToBottom = useCallback(() => {
    if (scrollRouteRef.current) {
      scrollRouteRef.current.scrollTop = scrollRouteRef.current.scrollHeight;
    }
  }, []);

  /**
   * スクロールイベントを処理します。
   * ユーザーが手動でスクロールした場合、自動スクロールを無効にします。
   *
   * @param {Event} event - スクロールイベント。
   */
  const handleScroll = () => {
    if (scrollRouteRef.current) {
      const { scrollTop, scrollHeight, clientHeight } = scrollRouteRef.current;
      // スクロールが最下部から一定距離以内に来た場合に判定を行う
      const scrollThreshold = 100;
      const isNearBottom =
        scrollHeight - scrollTop - scrollThreshold <= clientHeight;

      setCanScrollBottom(isNearBottom);
    }
  };

  /**
   * チャットログが更新されたときに自動的にスクロールを一番下まで行う
   *
   * このエフェクトは、チャットログが更新されたときに自動的にスクロールを
   * 一番下まで行うためのものです。ユーザーが手動でスクロールしている場合は、
   * 自動スクロールを無効にします。
   */
  // chatLogは参照していないが、chatLogが更新されたときにスクロールを行うために依存配列に含めています。
  // biome-ignore lint: lint/correctness/useExhaustiveDependencies
  useEffect(() => {
    if (canScrollBottom) {
      scrollToBottom();
    }
  }, [chatLog, canScrollBottom, scrollToBottom]);

  return { scrollRouteRef, handleScroll };
};