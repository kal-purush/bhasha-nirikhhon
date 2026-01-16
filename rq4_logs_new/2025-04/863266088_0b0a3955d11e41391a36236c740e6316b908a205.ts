import { create } from 'zustand';
import dayjs from 'dayjs';
import { useEffect } from 'react';
import useCountdown from '@/hooks/use-count-down';

// 定义结束时间：明天晚上8点
const getEndTimestamp = () => {
  return dayjs().add(1, 'day').hour(20).minute(0).second(0).unix();
};

// 测试模式设置
const TEST_MODE = false; 
const getTestEndTimestamp = () => dayjs().add(8, 'second').unix();

const getCurrentTimestamp = () => TEST_MODE ? getTestEndTimestamp() : getEndTimestamp();

interface CountdownState {
  endTimestamp: number;
  isEventEnded: boolean;
  setEventEnded: (value: boolean) => void;
}

export const useCountdownStore = create<CountdownState>((set) => ({
  endTimestamp: getCurrentTimestamp(),
  isEventEnded: false,
  setEventEnded: (value: boolean) => set({ isEventEnded: value }),
}));

export const useCountdownManager = () => {
  const { endTimestamp, isEventEnded, setEventEnded } = useCountdownStore();
  const { secondsRemaining } = useCountdown(endTimestamp);
  
  useEffect(() => {
    if (secondsRemaining <= 0 && !isEventEnded) {
      setEventEnded(true);
    }
  }, [secondsRemaining, isEventEnded, setEventEnded]);
  
  return {
    secondsRemaining,
    isEventEnded,
    endTimestamp
  };
};