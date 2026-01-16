export const environment = {
  production: false,
  authO: {
    issuer: 'https://dev-r1e4o7ny.us.auth0.com/',
    clientId: 'uBqZpyhO9dqVa1gBSPeysuzmKNPChgTq',
    redirectUri: 'https://my-angular-template.netlify.app/home',
    scope: 'openid profile email',
    response_type: 'id_token token',
    logoutUrl:
      'https://dev-r1e4o7ny.us.auth0.com/v2/logout?federated&returnTo=https://my-angular-template.netlify.app/landing&client_id=uBqZpyhO9dqVa1gBSPeysuzmKNPChgTq',
    postLogoutRedirectUri: 'https://my-angular-template.netlify.app/landing',
  },
};