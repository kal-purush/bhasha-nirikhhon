import { getOBSWebSocketConnection } from '../handlers/obs/obsWebsocket';
import type { BotCommand } from '../types';
import { hasBotCommandParams } from './helpers/hasBotCommandParams';
import { sendChatMessage } from './helpers/sendChatMessage';

export const coolestviewer: BotCommand = {
  command: ['coolestviewer'],
  id: 'coolestviewer',
  privileged: true,
  hidden: true,
  description: '',
  callback: (connection, parsedCommand) => {
    if (hasBotCommandParams(parsedCommand.parsedMessage)) {
      const obsWebSocket = getOBSWebSocketConnection();

      if (!obsWebSocket) {
        return;
      }

      const username = parsedCommand.parsedMessage.command.botCommandParams;

      obsWebSocket.send(
        JSON.stringify({
          op: 6,
          d: {
            requestType: 'GetInputSettings',
            requestId: 'get-input-settings',
            requestData: {
              inputName: 'Coolest Viewer',
            },
          },
        }),
      );

      obsWebSocket.send(
        JSON.stringify({
          op: 6,
          d: {
            requestType: 'SetInputSettings',
            requestId: 'set-coolest-viewer-name',
            requestData: {
              inputName: 'Coolest Viewer',
              inputSettings: {
                text: `Coolest Viewer: ${username}`,
              },
            },
          },
        }),
      );

      obsWebSocket.send(
        JSON.stringify({
          op: 6,
          d: {
            requestType: 'SetSceneItemEnabled',
            requestId: 'set-coolest-viewer-enabled',
            requestData: {
              sceneName: 'Live Scene',
              sceneItemId: 31,
              sceneItemEnabled: true,
            },
          },
        }),
      );

      sendChatMessage(connection, `${username} is today's coolest viewer`);
    }
  },
};