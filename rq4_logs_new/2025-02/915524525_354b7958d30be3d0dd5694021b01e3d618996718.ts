import { create } from "zustand";
import { EventSourcePolyfill } from "event-source-polyfill";

interface Message {
  id: string;
  message: string;
  type: string;
}

interface SSEState {
  messages: Message[];
  eventSource: EventSourcePolyfill;
  addMessage: (message: string, type: string) => void;
  removeMessage: (id: string) => void;
  connect: (token: string) => void;
  disconnect: () => void;
}

export const useSSEStore = create<SSEState>((set, get) => ({
  messages: [],
  eventSource: null,
  addMessage: (message, type) => {
    set((state) => ({
      messages: [...state.messages, { id: Date.now().toString(), message, type }],
    }));
    console.log("[SSE] messages:", get().messages);
  },
  removeMessage: (id) =>
    set((state) => ({
      messages: state.messages.filter((toast) => toast.id !== id),
    })),
  connect: (accessToken) => {
    if (get().eventSource || accessToken === null) return;
    console.log(accessToken);

    const eventSource = new EventSourcePolyfill(
      `${process.env.NEXT_PUBLIC_NOTI_URL}/api/user/sse/subscribe`,
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
        heartbeatTimeout: 60 * 1000,
        withCredentials: true,
      }
    );

    eventSource.onopen = () => console.log("[SSE] Connected");

    eventSource.onmessage = (event) => {
      get().addMessage(event.data, event.type);
    };

    eventSource.onerror = () => {
      eventSource.close();
      set({ eventSource: null });
      setTimeout(() => get().connect(accessToken), 2000);
    };

    if (get().eventSource) {
      get().disconnect();
    }
    set({ eventSource });

    return () => {
      eventSource.close();
      set({ eventSource: null });
    };
  },

  // TODO: 로그아웃 시 실행
  disconnect: () => {
    get().eventSource?.close();
    set({ eventSource: null });
  },
}));