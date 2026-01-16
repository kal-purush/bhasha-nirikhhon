module jDebug {

    export interface IJDebugMessage {
        type: number;
        data: any;
    }

    export class JDebugEventDispatcher {

        wsConnectionString:string;

        private ws:WebSocket;

        constructor() {
            var wsPrototocol = location.protocol === 'https:' ? 'wss:' : 'ws:';

            this.wsConnectionString = wsPrototocol + '//' + location.host + '/jdebug';

        }

        connect() {
            if (!WebSocket) {
                console.warn('jDebug: websockets is not supported to debug the application');
                return;
            }
            var isError = false;
            this.ws = new WebSocket(this.wsConnectionString);
            this.ws.onclose = ()=> {
                if (!isError) {
                    setTimeout(this.connect.bind(this), 5000);
                }
            };
            this.ws.onmessage = (event:MessageEvent)=> {
                this.dispatch(JSON.parse(event.data));
            };

            this.ws.onerror = ()=> {
                console.warn('jDebug: ws connection error');
                isError = true;
            };
        }

        private dispatch(message:IJDebugMessage) {
            switch (message.type) {
                case 1: // template changed
                    jDebug.components.updateComponentTemplateUrl(message.data.component);
                    break;
                case 2: // definition changed
                    if (message.data.type === 'component') {
                        jDebug.components.updateComponentDefinition(message.data);
                    }
                    break;
                case 3: // ctrl changed
                    if (message.data.def.type === 'component') {
                        jDebug.components.updateComponentDefinition(message.data.def, message.data.src);
                    }
                    break;
                case 4: // css changed
                    jDebug.styles.updateStyle(message.data);
                    break;
            }
            console.log('jDebug: message received:', message);
        }

    }
}