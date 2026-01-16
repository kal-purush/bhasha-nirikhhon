interface Auth0RedirectUrlParams {
  authorizeUrl: string;
  clientId: string;
  callbackUrl: string;
  state: string;
}

export function auth0RedirectUrlFactory(params: Auth0RedirectUrlParams) {
  const { authorizeUrl, clientId, callbackUrl, state } = params;

  function getRandomByRange(min: number, max: number) {
    return Math.floor(Math.random() * (max - min) + min);
  }

  return (connection: string) => {
    const nonce = getRandomByRange(10000, 99999);
    return `${authorizeUrl}?client_id=${clientId}&response_type=id_token&connection=${connection}&prompt=login&scope=openid profile email&redirect_uri=${callbackUrl}&state=${state}&nonce=${nonce}`;
  };
}