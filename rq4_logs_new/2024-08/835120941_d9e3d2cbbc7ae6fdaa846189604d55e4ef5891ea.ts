import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';

interface LiveChatState {
  email: string;
  fullName: string;
  channelId: string;
  createdAt: string;
  updateLiveChatState: (state: Partial<LiveChatState>) => void;
}

export const usePublicLiveChatStore = create<LiveChatState>()(
  persist(
    (set) => ({
      email: '',
      fullName: '',
      channelId: '',
      createdAt: new Date().toISOString(),
      updateLiveChatState: (state: Partial<LiveChatState>) => set(state),
    }),
    {
      name: 'public-livechat-popup',
      storage: createJSONStorage(() => localStorage),
    }
  )
);