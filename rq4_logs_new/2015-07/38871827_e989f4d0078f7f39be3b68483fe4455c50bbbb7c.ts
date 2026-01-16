declare module ho.flux {
    class CallbackHolder {
        protected prefix: string;
        protected lastID: number;
        protected callbacks: {
            [key: string]: Function;
        };
        register(callback: Function): string;
        unregister(id: any): void;
    }
}