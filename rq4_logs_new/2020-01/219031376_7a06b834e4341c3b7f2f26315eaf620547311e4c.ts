import { UserAgentApplication, AuthResponse } from 'msal';
import AzureADConfig from '../Configs/AzureADConfig';

class AuthService {
    private readonly _userAgentApp: UserAgentApplication;
    private readonly _scopes = [];

    constructor(userAgentApp: UserAgentApplication) {
        this._userAgentApp = userAgentApp;
    }

    public async LogIn(): Promise<AuthResponse> {
        return await this._userAgentApp.loginPopup();
    }

    public async GetAccessToken(): Promise<AuthResponse> {
        try {
            return await this._userAgentApp.acquireTokenSilent({ scopes: this._scopes });
        } catch (error) {
            return await this._userAgentApp.acquireTokenPopup({ scopes: this._scopes });
        }
    }

    public LogOut(): void {
        this._userAgentApp.logout();
    }
}

export default new AuthService(new UserAgentApplication(AzureADConfig));