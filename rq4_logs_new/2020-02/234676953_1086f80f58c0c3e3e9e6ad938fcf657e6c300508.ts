import request from 'request';
import { environment } from '@config/environment';
export function verifyFacebookToken(token: string): Promise<any> {
  return new Promise((resolve, reject) => {
    request.get(
      `https://graph.facebook.com/debug_token?input_token=${token}&access_token=${token}`,
      (req: any, response: any, next: any) => {
        const tokenResponse = JSON.parse(response.body);
        if (tokenResponse.error || !tokenResponse.data) {
          return reject(tokenResponse.error);
        }
        const appId = tokenResponse.data.app_id;
        const isValid = tokenResponse.data.is_valid;
        if (environment.FACEBOOK_CLIENT_ID === appId && isValid) {
          request.get(
            `https://graph.facebook.com/me/?access_token=${token}&fields=name,email,first_name,last_name`,
            (req2: any, response2: any, next2: any) => {
              const meResponse = JSON.parse(response2.body);
              return resolve(meResponse);
            }
          );
        } else {
          return reject();
        }
      }
    );
  });
}