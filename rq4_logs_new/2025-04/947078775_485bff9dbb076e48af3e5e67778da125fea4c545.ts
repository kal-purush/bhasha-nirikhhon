import { create } from 'zustand';

export type MessageVariant = 'default' | 'success' | 'error' | 'warning' | 'info';

export interface MessageState {
  isOpen: boolean;
  title?: string;
  message: string;
  variant: MessageVariant;
  buttonText?: string;
  onButtonClick?: () => void;

  // 액션
  openMessage: (props: {
    title?: string;
    message: string;
    variant?: MessageVariant;
    buttonText?: string;
    onButtonClick?: () => void;
  }) => void;
  closeMessage: () => void;
}

export const useMessageStore = create<MessageState>(set => ({
  isOpen: false,
  title: undefined,
  message: '',
  variant: 'default',
  buttonText: undefined,
  onButtonClick: undefined,

  openMessage: ({ title, message, variant = 'default', buttonText, onButtonClick }) => {
    set({
      isOpen: true,
      title,
      message,
      variant,
      buttonText,
      onButtonClick,
    });
  },

  closeMessage: () => {
    set({ isOpen: false });
  },
}));