import store from '~/src/utils/store';

export class WS {
    static basePath = 'wss://ya-praktikum.tech/ws/chats';

    static connTimerDelay = 5000;

    #connTimer;

    #chatId: number | null;

    #socket: WebSocket | null;

    constructor() {
        this.#connTimer = store?.getState()?.connTimer;
        this.#chatId = null;
        this.#socket = null;
    }

    #setTimer(): void {
        const connTimer = setInterval(() => {
            console.log(`sending ping message for ${this.#chatId}`);
            if (!this.#socket) {
                return;
            }
            this.#socket.send(
                JSON.stringify({
                    type: 'ping',
                })
            );
        }, WS.connTimerDelay);
        store.set('connTimer', connTimer);
    }

    #getMessages() {
        console.log(`get old messages for ${this.#chatId}`);
    }

    connect(chatId: number, token: string) {
        if (this.#connTimer) {
            clearInterval(this.#connTimer);
        }

        const userId = store?.getState()?.user?.id;
        this.#socket = new WebSocket(`${WS.basePath}/${userId}/${chatId}/${token}`);
        this.#chatId = chatId;

        this.#socket.addEventListener('open', () => {
            console.log('Websocket connection has been established');
            // get old messages
            this.#getMessages();
        });

        this.#socket.addEventListener('close', event => {
            if (event.wasClean) {
                console.log('Websocket connection has been closed without issues');
            } else {
                console.warn('Websocket connection has been close with issues');
            }

            console.log(`Code: ${event.code} | Reason: ${event.reason}`);
            // remove WS connection timer if exists to avoid pinging multiples chats
        });

        this.#socket.addEventListener('message', event => {
            console.log('Received data', event.data);
        });

        this.#socket.addEventListener('error', event => {
            console.log('Error', event.message);
        });

        this.#setTimer();
    }
}