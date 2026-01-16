import type { APIGatewayProxyEvent, APIGatewayProxyResult, Handler } from "aws-lambda"
import { formatJSONResponse } from '@libs/api-gateway';
import { middyfy } from '@libs/lambda';
import { BadRequest, Forbidden, MethodNotAllowed } from 'http-errors';
import passport from 'passport';
import { BasicStrategy } from 'passport-http';
import { UserAccessToken, toBasicAuthToken, decrypt } from '../../libs/user';
import datestr from '../../libs/utils/datestr';


const auth: Handler<APIGatewayProxyEvent, APIGatewayProxyResult> = async (event) => {
  
  /**
 * NOTE: we are NOT using Basic auth in a standard way.
 * The norm for HTTP Basic would be a token in format base64(username:password)
 * Here, we're expecting tokens in format base64(userid:accesstoken)
 */
passport.use(
  'basic',
  new BasicStrategy((userid, accesstoken, done) => {
    if (!userid) {
      return done(new BadRequest('basic access token is missing user id'));
    }
    const user: UserAccessToken = decrypt(accesstoken);
    if (!user) {
      return done(null, false);
    }
    if (user.id !== userid) {
      return done(new Forbidden());
    }
    return done(null, user);
  })
);

if (event.httpMethod !== 'POST') {
  return new MethodNotAllowed('only POST accepted for this endpoint');
}
const username = event.queryStringParameters['username'] as string;
if (!username) {
  return new BadRequest("missing required query param 'username'");
}
const userid = event.queryStringParameters['userid'] as string;
if (!userid) {
  return new BadRequest("missing required query param 'userid'");
}
return formatJSONResponse({
  'auth-token': toBasicAuthToken({
    name: username,
    id: userid,
    expiresAt: datestr(new Date(Date.now() + 60 * 60 * 24)),
  }),
});
};

export const main = middyfy(auth);