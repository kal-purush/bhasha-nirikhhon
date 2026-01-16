import { APIGatewayTokenAuthorizerEvent } from 'aws-lambda';

export const basicAuthorizer = (event: APIGatewayTokenAuthorizerEvent, _context, cb) => {
    console.log(`Event: ${JSON.stringify(event)}`);

    if (event.type !== 'TOKEN') {
        cb('Unauthorized');
    }

    try {
        const { authorizationToken } = event;
        const encodedCreds = authorizationToken.split(' ')[1];
        const buff = Buffer.from(encodedCreds, 'base64');

        const [username, password] = buff.toString('utf-8').split(':');

        console.log(`Username = ${username} and password = ${password}`);

        const storedPassword = process.env[username];

        const effect = !storedPassword || password !== storedPassword ? 'Deny' : 'Allow';

        const policy = generatePolicy(encodedCreds, event.methodArn, effect);
        cb(null, policy);
    } catch (err) {
        cb(`Unauthorized: ${err.message}`);
    }
}

  const generatePolicy = (principalId, resource, effect) => {
    return {
        principalId,
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
};