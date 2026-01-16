import Keycloak, { KeycloakInitOptions, KeycloakInstance } from 'keycloak-js';

// =================================================================================================

let keycloak: KeycloakInstance | undefined;

// =================================================================================================

export function keycloakInit(
  options = {
    onLoad: 'login-required',
    checkLoginIframe: false,
  } as KeycloakInitOptions,
): Promise<KeycloakInstance> {
  return new Promise((resolve, reject) => {
    const keycloakAuth = (keycloak = Keycloak());
    function init() {
      keycloakAuth
        .init(options)
        .success(authenticated => {
          console.error('Keycloak initialization successful:', authenticated);
          resolve(keycloakAuth);
        })
        .error((errorData: any) => {
          console.error('Error during Keycloak initialization:', errorData);
          reject(errorData);
        });
    }

    keycloakAuth.onAuthSuccess = () => console.log('onAuthSuccess', arguments);
    keycloakAuth.onAuthError = () => console.log('onAuthError', arguments);
    keycloakAuth.onAuthLogout = () => console.log('onAuthLogout', arguments);
    keycloakAuth.onAuthRefreshSuccess = () =>
      console.log('onAuthRefreshSuccess', arguments);
    keycloakAuth.onAuthRefreshError = () => {
      console.log('onAuthRefreshError', arguments);
      init();
    };
    init();
  });
}

export function useBearer(): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    if (keycloak && keycloak.token) {
      const keycloakAuth = keycloak;
      keycloakAuth
        .updateToken(5)
        .success(() => {
          resolve(keycloakAuth.token);
        })
        .error(() => {
          reject('Failed to refresh token');
        });
    } else {
      reject('Not logged in');
    }
  });
}