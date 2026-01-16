import type { Serverless } from 'serverless/aws';
import { baseServerlessConfiguration } from '../../serverless.base';

export const serverlessConfig: Serverless = {
  ...baseServerlessConfiguration,
  service: 'hello-world',
  provider: {
    ...baseServerlessConfiguration.provider,
    apiGateway: {
      minimumCompressionSize: 1024,
      // @ts-ignore
      restApiId: {
        'Fn::ImportValue': `dev-AppApiGW-restApiId`,
      },
      // @ts-ignore
      restApiRootResourceId: {
        'Fn::ImportValue': `dev-AppApiGW-rootResourceId`,
      },
    },
  },
  functions: {
    hello: {
      handler: 'src/app.main',
      events: [
        {
          http: {
            method: 'get',
            path: 'user',
          },
        },
      ],
    }
  }
};

module.exports = serverlessConfig