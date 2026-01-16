import { APIGatewayAuthorizerHandler, APIGatewayAuthorizerResult } from 'aws-lambda';


export const basicAuthorizer: APIGatewayAuthorizerHandler = (event, _, callback) => {
  console.log('Lambda invocation with event: ', event);

  if (event.type !== 'TOKEN') {
    return callback('Unauthorized');
  }

  const token = event.authorizationToken.split(' ')[1];
  const credentials = parseToken(token);

  const storedUserPassword = process.env[credentials.userName];
  const effect = storedUserPassword && storedUserPassword === credentials.password ? 'Allow' : 'Deny';
  const policy = generatePolicy(token, event.methodArn, effect);

  return callback(null, policy);
}

function parseToken(token: string) {
  const buffer = Buffer.from(token, 'base64');
  const plainCredentials = buffer.toString('utf-8').split(':');

  return {
    userName: plainCredentials[0],
    password: plainCredentials[1]
  }
}

function generatePolicy(principalId: string, resource: string, effect: 'Allow' | 'Deny'): APIGatewayAuthorizerResult {
  return {
    principalId,
    policyDocument: {
      Version: '2012-10-17',
      Statement: [{
        Action: 'execute-api:Invoke',
        Effect: effect,
        Resource: resource
      }]
    }
  }
}