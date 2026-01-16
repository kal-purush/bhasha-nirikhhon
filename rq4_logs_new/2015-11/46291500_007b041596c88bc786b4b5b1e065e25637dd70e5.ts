
abstract class AbstractOauthProvider implements OauthProviderConnector {

    // By default we store the token in sessionStorage. This can be overridden in init()
    tokenStore: Storage = window.sessionStorage;

    context: string = window.location.pathname.substring(0, window.location.pathname.lastIndexOf("/"));

    baseURL: string = location.protocol + '//' + location.hostname + (location.port ? ':' + location.port : '') + this.context;

    // Default OAuth redirect URL. Can be overriden in init()
    oauthRedirectURL: string = this.baseURL + '/oauthcallback.html';

    // Default Cordova OAuth redirect URL. Can be overriden in init()
    cordovaOAuthRedirectURL: string = "https://www.facebook.com/connect/login_success.html";

    // Default Logout redirect URL. Can be overriden in init()
    logoutRedirectURL: string = this.baseURL + '/logoutcallback.html';

    // Because the OAuth login spans multiple processes, we need to keep the login callback function as a variable
    // inside the module instead of keeping it local within the login function.
    loginCallback;

    // Indicates if the app is running inside Cordova
    runningInCordova;

    // Used in the exit event handler to identify if the login has already been processed elsewhere (in the oauthCallback function)
    loginProcessed;

    // MAKE SURE YOU INCLUDE <script src="cordova.js"></script> IN YOUR index.html, OTHERWISE runningInCordova will always by false.
    // You don't need to (and should not) add the actual cordova.js file to your file system: it will be added automatically
    // by the Cordova build process
    //document.addEventListener("deviceready", function () {
    //    runningInCordova = true;
    //}, false);

    /**
     * Initialize the common functionality of the provider. You must use this function and initialize the module with an appId before you can use any other function.
     * @param params - init parameters
     *  tokenStore: (optional) The store used to save the token. If not provided, we use sessionStorage.
     *  loginURL: (optional) The OAuth login URL. Defaults to https://www.facebook.com/dialog/oauth.
     *  logoutURL: (optional) The logout URL. Defaults to https://www.facebook.com/logout.php.
     *  oauthRedirectURL: (optional) The OAuth redirect URL. Defaults to [baseURL]/oauthcallback.html.
     *  cordovaOAuthRedirectURL: (optional) The OAuth redirect URL. Defaults to https://www.facebook.com/connect/login_success.html.
     *  logoutRedirectURL: (optional) The logout redirect URL. Defaults to [baseURL]/logoutcallback.html.
     *  accessToken: (optional) An already authenticated access token.
     */
    init(params): void {

        if (params.tokenStore) {
            this.tokenStore = params.tokenStore;
        }

        if (params.accessToken) {
            this.tokenStore.setItem('accessToken', params.accessToken);
        }

        this.oauthRedirectURL = params.oauthRedirectURL || this.oauthRedirectURL;
        this.cordovaOAuthRedirectURL = params.cordovaOAuthRedirectURL || this.cordovaOAuthRedirectURL;
        this.logoutRedirectURL = params.logoutRedirectURL || this.logoutRedirectURL;

    }

    /**
     * Checks if the user has logged in with openFB and currently has a session api token.
     * @param callback the function that receives the loginstatus
     */
    getLoginStatus(callback): void {
        var token = this.tokenStore.getItem('accessToken'),
            loginStatus = new LoginStatus();
        if (token) {
            loginStatus.status = 'connected';
            loginStatus.authResponse = {accessToken: token};
        } else {
            loginStatus.status = 'unknown';
        }
        if (callback) callback(loginStatus);
    }

    /**
     * Login to Facebook using OAuth. If running in a Browser, the OAuth workflow happens in a a popup window.
     * If running in Cordova container, it happens using the In-App Browser. Don't forget to install the In-App Browser
     * plugin in your Cordova project: cordova plugins add org.apache.cordova.inappbrowser.
     *
     * @param callback - Callback function to invoke when the login process succeeds
     * @param options - options.scope: The set of Facebook permissions requested
     * @returns {*}
     */
    login(callback, options) {

        var loginWindow,
            startTime,
            scope = '',
            redirectURL = this.runningInCordova ? this.cordovaOAuthRedirectURL : this.oauthRedirectURL;

        var errors = this.getSetupErrors();
        if(errors.length > 0) {
            return callback({status: 'unknown', error: errors[0]});
        }

        // Inappbrowser load start handler: Used when running in Cordova only
        function loginWindow_loadStartHandler(event) {
            var url = event.url;
            if (url.indexOf("access_token=") > 0 || url.indexOf("error=") > 0) {
                // When we get the access token fast, the login window (inappbrowser) is still opening with animation
                // in the Cordova app, and trying to close it while it's animating generates an exception. Wait a little...
                var timeout = 600 - (new Date().getTime() - startTime);
                setTimeout(function () {
                    loginWindow.close();
                }, timeout > 0 ? timeout : 0);
                this.oauthCallback(url);
            }
        }

        // Inappbrowser exit handler: Used when running in Cordova only
        function loginWindow_exitHandler() {
            console.log('exit and remove listeners');
            // Handle the situation where the user closes the login window manually before completing the login process
            if (this.loginCallback && !this.loginProcessed) this.loginCallback({status: 'user_cancelled'});
            loginWindow.removeEventListener('loadstop', this.loginWindow_loadStopHandler);
            loginWindow.removeEventListener('exit', loginWindow_exitHandler);
            loginWindow = null;
            console.log('done removing listeners');
        }

        if (options && options.scope) {
            scope = options.scope;
        }

        this.loginCallback = callback;
        this.loginProcessed = false;

        startTime = new Date().getTime();
        loginWindow = window.open(this.getLoginUrl(redirectURL));

        // If the app is running in Cordova, listen to URL changes in the InAppBrowser until we get a URL with an access_token or an error
        if (this.runningInCordova) {
            loginWindow.addEventListener('loadstart', loginWindow_loadStartHandler);
            loginWindow.addEventListener('exit', loginWindow_exitHandler);
        }
        // Note: if the app is running in the browser the loginWindow dialog will call back by invoking the
        // oauthCallback() function. See oauthcallback.html for details.

    }

    abstract getSetupErrors(): string[];

    abstract getLoginUrl(redirectURL: string): string;

    /**
     * Called either by oauthcallback.html (when the app is running the browser) or by the loginWindow loadstart event
     * handler defined in the login() function (when the app is running in the Cordova/PhoneGap container).
     * @param url - The oautchRedictURL called by Facebook with the access_token in the querystring at the ned of the
     * OAuth workflow.
     */
    abstract oauthCallback(url): void;

    resolveConnectedCallback(authResponse) {
        if (this.loginCallback) this.loginCallback({status: 'connected', authResponse: authResponse});
    }

    resolveNotAuthorizedCallback(error) {
        if (this.loginCallback) this.loginCallback({status: 'not_authorized', error: error});
    }

    /**
     * Logout from Facebook, and remove the token.
     * IMPORTANT: For the Facebook logout to work, the logoutRedirectURL must be on the domain specified in "Site URL" in your Facebook App Settings
     *
     */
    logout(callback) {
        var logoutWindow,
            token = this.tokenStore.getItem('accessToken');

        /* Remove token. Will fail silently if does not exist */
        this.tokenStore.removeItem('fbtoken');

        if (token) {
            logoutWindow = window.open(this.getLogoutUrl(token));
            if (this.runningInCordova) {
                setTimeout(function() {
                    logoutWindow.close();
                }, 700);
            }
        }

        if (callback) {
            callback();
        }

    }

    abstract getLogoutUrl(token): string;

    /**
     * Lets you make any Facebook Graph API request.
     * @param obj - Request configuration object. Can include:
     *  method:  HTTP method: GET, POST, etc. Optional - Default is 'GET'
     *  path:    path in the Facebook graph: /me, /me.friends, etc. - Required
     *  params:  queryString parameters as a map - Optional
     *  success: callback function when operation succeeds - Optional
     *  error:   callback function when operation fails - Optional
     */
    api(obj) {

        var method = obj.method || 'GET',
            params = obj.params || {},
            xhr = new XMLHttpRequest(),
            url;

        params['access_token'] = this.tokenStore.getItem('accessToken');

        url = 'https://api.github.com' + obj.path + '?' + AbstractOauthProvider.toQueryString(params);

        xhr.onreadystatechange = function () {
            if (xhr.readyState === 4) {
                if (xhr.status === 200) {
                    if (obj.success) obj.success(JSON.parse(xhr.responseText));
                } else {
                    var error = xhr.responseText ? JSON.parse(xhr.responseText) : {message: 'An error has occurred'};
                    if (obj.error) obj.error(error);
                }
            }
        };

        xhr.open(method, url, true);
        xhr.send();
    }

    /**
     * Helper function to de-authorize the app
     * @param success
     * @param error
     * @returns {*}
     */
    revokePermissions(success, error) {
        return this.api({method: 'DELETE',
            path: '/me/permissions',
            success: function () {
                success();
            },
            error: error});
    }


    protected static parseQueryString(queryString) {
        var qs = decodeURIComponent(queryString),
            obj = {},
            params = qs.split('&');
        params.forEach(function (param) {
            var splitter = param.split('=');
            obj[splitter[0]] = splitter[1];
        });
        return obj;
    }

    private static toQueryString(obj): string {
        var parts = [];
        for (var i in obj) {
            if (obj.hasOwnProperty(i)) {
                parts.push(encodeURIComponent(i) + "=" + encodeURIComponent(obj[i]));
            }
        }
        return parts.join("&");
    }

    protected static randomString(length): string {
        var options = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        return new Array(length).join().split(',').map(function() { return options.charAt(Math.floor(Math.random() * options.length)); }).join('');
    }

}