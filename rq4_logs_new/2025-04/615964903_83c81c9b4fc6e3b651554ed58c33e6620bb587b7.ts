/// <reference types="vite/client" />

interface TelegramUser {
    id: number;
    first_name: string;
    username?: string;
    last_name?: string;
    language_code?: string;
    is_premium?: boolean;
}

interface TelegramWebApp {
    ready: () => void;
    sendData: (data: string) => void;
    initDataUnsafe: {
        user?: TelegramUser;
    };
}

interface Window {
    Telegram?: {
        WebApp: TelegramWebApp;
    };
}