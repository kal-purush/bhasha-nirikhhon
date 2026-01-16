import { create } from 'zustand';

type Notification = {
  id: string;
  success: boolean;
  message: string;
  timestamp: number;
};

type MessageStore = {
  notifications: Notification[];
  showMessage: (success: boolean, message: string, id: string) => void;
  dismissMessage: (id: string) => void;
};

export const useMessageStore = create<MessageStore>((set) => ({
  notifications: [],
  
  showMessage: (success, message, id) => {
    const newNotification: Notification = {
      id,
      success,
      message,
      timestamp: Date.now()
    };
    
    set((state) => ({
      notifications: [...state.notifications, newNotification]
    }));
  },
  
  dismissMessage: (id) => {
    set((state) => ({
      notifications: state.notifications.filter(n => n.id !== id)
    }));
  },
  
}));