import {
    AuthorizationServiceConfiguration,
    AuthorizationRequest,
    RedirectRequestHandler,
    AuthorizationNotifier,
    BaseTokenRequestHandler,
    FetchRequestor,
    TokenRequest,
    GRANT_TYPE_AUTHORIZATION_CODE,
    TokenResponse
} from '@openid/appauth';
import { TokenRequestHandler } from '@openid/appauth/src/token_request_handler';

export default class OAuthAuthentication {
    private readonly _authorizationServiceConfiguration: AuthorizationServiceConfiguration;
    private readonly _redirectRequestHandler: RedirectRequestHandler = new RedirectRequestHandler;
    private readonly _tokenRequestHandler: TokenRequestHandler = new BaseTokenRequestHandler(new FetchRequestor(),);
    private readonly _issuerEndpoint: string;
    constructor(protected readonly issuerEndpoint: string, protected readonly authorizationEndpoint: string, protected readonly tokenEndpoint: string, protected readonly revocationEndpoint: string) {
        this._issuerEndpoint = issuerEndpoint;
        this._authorizationServiceConfiguration = new AuthorizationServiceConfiguration({
            authorization_endpoint: authorizationEndpoint,
            token_endpoint: tokenEndpoint,
            revocation_endpoint: revocationEndpoint
        });
    }

    public async Authorize(clientId: string, scopes: string[]): Promise<TokenResponse> {
        const redirectUrl = new URL(window.location.toString());
        let authorizationRequest = new AuthorizationRequest({
            client_id: clientId,
            redirect_uri: redirectUrl.toString(),
            scope: scopes.join(' '),
            response_type: AuthorizationRequest.RESPONSE_TYPE_CODE
        });
        await authorizationRequest.setupCodeVerifier();
        this._redirectRequestHandler.performAuthorizationRequest(this._authorizationServiceConfiguration, authorizationRequest);
        return await this.GetToken(clientId);
    }

    private async GetToken(clientId: string): Promise<TokenResponse> {
        const authorizationNotifier = new AuthorizationNotifier();
        this._redirectRequestHandler.setAuthorizationNotifier(authorizationNotifier);

        await this._redirectRequestHandler.completeAuthorizationRequestIfPossible();
        let tokenRequest: any;
        authorizationNotifier.setAuthorizationListener((request, response, error) => {
            // response object returns code which is in URL i.e. response.code
            // request object returns code_verifier i.e request.internal.code_verifier
            // you will need to add here token request process

            // Exchange for an access token and return tokenResponse
            tokenRequest = new TokenRequest({
                client_id: clientId,
                redirect_uri: this._issuerEndpoint,
                grant_type: GRANT_TYPE_AUTHORIZATION_CODE,
                code: response?.code,
                extras: {
                    // code_verifier should always be specified, but this is a safer runtime check
                    code_verifier: request.internal?.code_verifier ?? '',
                }
            });
        });
        return this._tokenRequestHandler.performTokenRequest(this._authorizationServiceConfiguration, tokenRequest);
    }
}