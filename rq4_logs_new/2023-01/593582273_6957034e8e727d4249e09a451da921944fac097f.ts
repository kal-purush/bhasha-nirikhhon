import nookies from 'nookies';
import {HttpClient} from '../../src/infra/HttpClient/httpClient';
import {tokenService} from '../../src/services/auth/tokenService';

const REFRESH_TOKEN_NAME = 'REFRESH_TOKEN_NAME';

const controllers = {
  // Guarda o refresh token
  async storeRefreshToken(req, res) {
    const context = {req, res};
    //console.log(req.body);
    
    nookies.set(context, REFRESH_TOKEN_NAME, req.body.refresh_token, {
      httpOnly: true,
      sameSite: 'lax',
      path: '/'
    });
    
    res.json({
      data: {
        message: 'Stored with sucess'
      }
    });
  },
  async displayCookies(req, res) {
    const context = {req, res};
    res.json({
      data: {
        cookies: nookies.get(context)
      }
    });
  },
  async regenerateTokens(req, res) {
    const context = {req, res};
    const cookies = nookies.get(context);
    const refresh_token = cookies[REFRESH_TOKEN_NAME] || req.body.refresh_token;
    console.log('/api/refresh [regenerateTokens]', refresh_token);

    const refreshResponse = await HttpClient(`${process.env.NEXT_PUBLIC_BACKEND_URL}/api/refresh`, {
      method: 'POST',
      body: {
        refresh_token
      }
    });
    
    if(refreshResponse.ok) {
      nookies.set(context, REFRESH_TOKEN_NAME, refreshResponse.body.data.refresh_token, {
        httpOnly: true,
        sameSite: 'lax',
        path: '/'
      });
      
      tokenService.save(refreshResponse.body.data.refresh_token, context);

      res.status(200).json({
        data: refreshResponse.body.data
      });
    } else {
      res.status(401).json({
        status: 401,
        message: 'Não autorizado'
      });
    }
    

  }
};

const controllerBy = {
  POST: controllers.storeRefreshToken,
  GET: controllers.regenerateTokens,
  PUT: controllers.regenerateTokens
};

export default function handler(request, response) {
  if(controllerBy[request.method]) {
    return controllerBy[request.method](request, response);
  }
  response.status(404).json({
    status: 404,
    message: 'Not Found'
  });
}