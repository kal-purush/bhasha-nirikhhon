import { create } from 'zustand';
import { User, Message } from '../types';
import { rateLimiter, isSpam, sanitizeInput } from '../utils/security';
import { cleanupMessages } from '../utils/messageCleanup';

interface ChatStore {
  user: User | null;
  messages: Message[];
  setUser: (name: string, avatar?: string) => void;
  logout: () => void;
  sendMessage: (content: string) => boolean;
}

export const useChatStore = create<ChatStore>((set, get) => ({
  user: null,
  messages: [],
  setUser: (name: string, avatar?: string) => {
    const sanitizedName = sanitizeInput(name);
    if (sanitizedName.length < 2) return;
    
    set({
      user: {
        id: Math.random().toString(36).substring(2),
        name: sanitizedName,
        avatar,
        lastActive: new Date()
      }
    });
  },
  logout: () => set({ user: null }),
  sendMessage: (content: string) => {
    const user = get().user;
    if (!user) return false;

    const sanitizedContent = sanitizeInput(content);
    if (!sanitizedContent || isSpam(sanitizedContent)) return false;
    
    if (!rateLimiter.canSendMessage(user.id)) return false;
    
    rateLimiter.recordMessage(user.id);
    
    const newMessage: Message = {
      id: Math.random().toString(36).substring(2),
      userId: user.id,
      userName: user.name,
      userAvatar: user.avatar,
      content: sanitizedContent,
      timestamp: new Date()
    };

    set(state => ({
      messages: cleanupMessages([...state.messages, newMessage])
    }));

    return true;
  }
}));