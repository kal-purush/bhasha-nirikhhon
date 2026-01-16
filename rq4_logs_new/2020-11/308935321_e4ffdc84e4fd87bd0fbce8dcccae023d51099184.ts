import 'source-map-support/register';
import {
  APIGatewayAuthorizerResult,
  APIGatewayTokenAuthorizerHandler,
} from 'aws-lambda';

// TODO: check async
export const basicAuthorizer: APIGatewayTokenAuthorizerHandler = (
  event,
  _ctx,
  cb
) => {
  console.log('Event: ', event);

  if (event.type !== 'TOKEN') {
    cb('Unauthorized');
  }

  try {
    const { authorizationToken, methodArn } = event;
    const encodedCreds = authorizationToken.split(' ')[1];
    const buff = Buffer.from(encodedCreds, 'base64');
    const [username, password] = buff.toString('utf-8').split(':');

    console.log(`Username: ${username}, Password: ${password}`);

    const storedUserPassword = process.env[username];
    const effect =
      storedUserPassword && storedUserPassword === password ? 'Allow' : 'Deny';
    const policy = generatePolicy(encodedCreds, methodArn, effect);

    cb(null, policy);
  } catch (error) {
    cb('Unauthorized: ', error.message);
  }
};

function generatePolicy(
  principalId: string,
  resource: string,
  effect: string
): APIGatewayAuthorizerResult {
  return {
    principalId: principalId,
    policyDocument: {
      Version: '2012-10-17',
      Statement: [
        {
          Action: 'execute-api:Invoke',
          Effect: effect,
          Resource: resource,
        },
      ],
    },
  };
}