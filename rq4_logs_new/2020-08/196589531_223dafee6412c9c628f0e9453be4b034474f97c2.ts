import { _SSOConfig } from 'src/public_api';

export const ssoConfig: _SSOConfig = {
  apiPath: 'https://sso.fake.net',
  clientID: 'fake_client',
  clientSecret: 'LoremIpsumDolorSitAmetConseceteturAdipiscingElitSedDoEiusmodTemp',
  grantType: 'password',
  authedPaths: ['https://wizsolucoes.com.br/'],
  scope: 'my.scope',
  options: {
    ssoTimeOut: 60000, // parâmetro opcional, determina o timeout para o SSO
    tokenAutoRefresh: true, // parâmetro opcional, determina se o token deve ser renovado
    loginRoute: 'login', // url que aponta para onde redirecionar no caso de não haver token
  },
};