import type { MockServerConfig } from 'mock-config-server';
import { createMockServer, startMockServer } from 'mock-config-server';

export const mockServerConfig: MockServerConfig = {
  graphql: {
    configs: [
      {
        operationType: 'query',
        operationName: 'GetUsers',
        routes: [
          {
            data: [{ id: 1, emoji: '🎉' }]
          }
        ]
      },
      {
        operationType: 'mutation',
        operationName: 'CreateUser',
        routes: [
          {
            data: { id: 1, emoji: '🎉' }
          }
        ]
      },
      {
        operationType: 'mutation',
        operationName: 'DeleteUser',
        routes: [
          {
            data: { succes: true }
          }
        ]
      },
      {
        operationType: 'mutation',
        operationName: 'ChangeUser',
        routes: [
          {
            data: { id: 1, emoji: '🎉' }
          }
        ]
      }
    ]
  }
};

createMockServer(mockServerConfig);
startMockServer(mockServerConfig);