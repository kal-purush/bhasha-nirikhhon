import { useEffect, useState } from 'react';

import { DEFAULT_OPTIONS, Data, Options, Snowflake } from '@/types/lanyard';

export enum SocketOpcode {
  Event,
  Hello,
  Initialize,
  Heartbeat,
}

export enum SocketEvents {
  INIT_STATE = 'INIT_STATE',
  PRESENCE_UPDATE = 'PRESENCE_UPDATE',
}

export interface SocketData extends Data {
  heartbeat_interval?: number;
}

export interface SocketMessage {
  d?: SocketData;
  op: SocketOpcode;
  t?: SocketEvents;
}

export function useLanyardWS(snowflake: Snowflake | Snowflake[], _options?: Partial<Options>) {
  const options = {
    ...DEFAULT_OPTIONS,
    ..._options,
  };

  const [data, setData] = useState<Data>();

  const protocol = options.api.secure ? 'wss' : 'ws';
  const url = `${protocol}://${options.api.hostname}/socket`;

  useEffect(() => {
    // Don't try to connect on server
    if (typeof window === 'undefined') {
      return;
    }

    if (!('WebSocket' in window || 'MozWebSocket' in window)) {
      throw new Error('WebSocket connections not supported in this browser.');
    }

    let subscribe_data: { subscribe_to_id?: string; subscribe_to_ids?: string[] };

    if (Array.isArray(snowflake)) {
      subscribe_data = { subscribe_to_ids: snowflake };
    } else {
      subscribe_data = { subscribe_to_id: snowflake };
    }

    let heartbeat: ReturnType<typeof setTimeout>;
    let socket: WebSocket;

    function connect() {
      if (heartbeat) {
        clearInterval(heartbeat);
      }

      socket = new WebSocket(url);

      socket.addEventListener('open', () => {
        console.log('Lanyard: Socket connection opened');
      });

      socket.addEventListener('close', connect);

      socket.addEventListener('message', (event) => {
        const message = JSON.parse(event.data) as SocketMessage;
        console.log('Lanyard: Received message', message); // Log the received message

        switch (message.op) {
          case SocketOpcode.Hello: {
            heartbeat = setInterval(() => {
              if (socket.readyState === socket.OPEN) {
                socket.send(JSON.stringify({ op: SocketOpcode.Heartbeat }));
              }
            }, message.d?.heartbeat_interval);

            if (socket.readyState === socket.OPEN) {
              socket.send(
                JSON.stringify({
                  d: subscribe_data,
                  op: SocketOpcode.Initialize,
                })
              );
            }

            break;
          }

          case SocketOpcode.Event: {
            switch (message.t) {
              case SocketEvents.INIT_STATE:
              case SocketEvents.PRESENCE_UPDATE: {
                if (message.d) {
                  console.log('Lanyard: Setting data', message.d); // Log the data being set
                  setData(message.d);
                } else {
                  console.error('Lanyard: Received undefined data', message); // Log the error if data is undefined
                }

                break;
              }

              default: {
                break;
              }
            }

            break;
          }

          default: {
            break;
          }
        }
      });
    }

    connect();

    return () => {
      clearInterval(heartbeat);

      socket.removeEventListener('close', connect);
      socket.close();
    };
  }, [url, snowflake]);

  return data ?? options.initialData;
}