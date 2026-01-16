import { CustomAuthorizerHandler, CustomAuthorizerEvent, CustomAuthorizerResult } from 'aws-lambda'
import 'source-map-support/register'

import { verify } from 'jsonwebtoken'
import { JwtToken } from '../../auth/JwtToken'

const cert = `-----BEGIN CERTIFICATE-----
MIIDBzCCAe+gAwIBAgIJHzAtIAKqys78MA0GCSqGSIb3DQEBCwUAMCExHzAdBgNV
BAMTFmRldi00ajd3N2wzZC5hdXRoMC5jb20wHhcNMjAwNDE2MTUwMTMzWhcNMzMx
MjI0MTUwMTMzWjAhMR8wHQYDVQQDExZkZXYtNGo3dzdsM2QuYXV0aDAuY29tMIIB
IjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5NdHdtfghH8t1xplljbdsGXK
UTjKoyOoPkrFW+DLe+4rMeNxYomzrulzRlW/bCiYrr55Ly0wfkQTc5KdsnVWwwVj
iIq3uHhXyhUhq2Bajm5gJfyQzGS4/ES8ZLkOcn0sdR4k6nJTO7lVM9n8YnE7SQtw
9hT4JSdbRKGQJ1nJZoe1SN7pGHEo9jniVTnN6wDHSSlPetfkBqYlxEPqLUXwi8IX
DmhHDCHF6PoGv4qsnpoAeC/LIqkQIpxNhC8DjDBLp20f++s6+b39V5MO8GDKOWTl
h+9f5dwn8Ut4nffbvzC7JZxCCRRAAGDAxA31+ebj4FueBkib+hE4W14iYUzXowID
AQABo0IwQDAPBgNVHRMBAf8EBTADAQH/MB0GA1UdDgQWBBQGCkLsfQYimxh2CCtl
Abln2eSm+DAOBgNVHQ8BAf8EBAMCAoQwDQYJKoZIhvcNAQELBQADggEBALLlnayR
vqz+7xFNy+dNXzpxWtzXDKJzKtWWbRYn3ZB0DH6RlQbjE+k/i3CNfJBOLTPxDdZq
hftKWrN71AGW5+1Rr44iNdWV5YDgTNu1QkIXcgi36iHtZQSCjr4zbI3KdBzsQwHI
d/4oVXdfAmZSFJVqbM7SwxifJ9S64x0kIx4mQQ3FbLRZY5Mgo8DyVSKA/0CVXQBm
sqVYvvUtSEN4gCh1JTEusQFg1GszmjCrugsjqOBa7to6hFwu4wnfWFXdzZX/r3RK
rWFgYxg51hT8+u1W+glNy9LXpfA9xgWvMAnyf/r+ikWgmKqrds1d9fRz2jdLN2GA
RC+wY4GYt7/GNrY=
-----END CERTIFICATE-----`


export const handler: CustomAuthorizerHandler = async (event: CustomAuthorizerEvent): Promise<CustomAuthorizerResult> => {
  try {
    const decodedToken = verifyToken(
      event.authorizationToken
    )
    console.log('User was authorized')

    return {
      principalId: decodedToken.sub,
      policyDocument: {
        Version: '2012-10-17',
        Statement: [
          {
            Action: 'execute-api:Invoke',
            Effect: 'Allow',
            Resource: '*'
          }
        ]
      }
    }
  } catch (e) {
    console.log('User was not authorized', e.message)

    return {
      principalId: 'user',
      policyDocument: {
        Version: '2012-10-17',
        Statement: [
          {
            Action: 'execute-api:Invoke',
            Effect: 'Deny',
            Resource: '*'
          }
        ]
      }
    }
  }
}

function verifyToken(authHeader: string): JwtToken {
  if (!authHeader) {
    throw new Error('No auth header')
  }
  if (!authHeader.toLocaleLowerCase().startsWith('bearer ')) {
    throw new Error('Invalid auth header')
  }
  
  const split = authHeader.split(' ')
  const token = split[1]

  return verify(
    token,           // Token from an HTTP header to validate
    cert,            // A certificate copied from Auth0 website
    { algorithms: ['RS256'] } // We need to specify that we use the RS256 algorithm
  ) as JwtToken as JwtToken

  // A request has been authorized.
}
