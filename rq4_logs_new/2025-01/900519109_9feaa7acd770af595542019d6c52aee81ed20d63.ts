import { create } from 'zustand';

import { TodayCheckResponse } from '@/apis/dailyCheck/type';

interface TodayCheckState {
  todayCheckData: TodayCheckResponse[] | null;
  setTodayCheckData: (value: TodayCheckResponse[]) => void;
}

/** 메인에서 관리되는 회원의 일일점검 현황 */
export const useTodayCheck = create<TodayCheckState>((set) => ({
  /** 로그인한 회원의 일일점검 데이터 */ todayCheckData: null,
  setTodayCheckData: (value: TodayCheckResponse[]) => set({ todayCheckData: value }),
}));