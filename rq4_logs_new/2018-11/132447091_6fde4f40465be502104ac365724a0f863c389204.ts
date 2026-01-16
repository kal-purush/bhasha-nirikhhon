// eslint-disable-next-line filenames/match-exported
import { serialize, parse } from 'cookie';
import { Api } from 'rest-api-handler';

export default class CookieApi<ResponseType> extends Api<ResponseType> {
    /**
     * Serialize Object of cookies to single string.
     *
     * @param cookies - Object with cookies
     * @returns serialized cookies string
     */
    public static serializeCookies(cookies: { [cookie: string]: string }): string {
        return Object.keys(cookies).map((name) => {
            return serialize(name, cookies[name]);
        }).join(';');
    }

    /**
     * Process response headers, parse cookies and save them to object.
     *
     * @param request - Fetch request
     * @returns Response
     */
    protected fetchRequest: (request: Request) => Promise<Response> = async (request) => {
        const response = await super.fetchRequest.call(this, request);

        const setCookieHeader = response.headers.get('set-cookie');

        // if set cookies headers are not set, just continue
        if (!setCookieHeader) {
            return response;
        }

        let cookies = {};

        // parse multiple set-cookie headers
        setCookieHeader.split(';')
            .map((item: string) => {
                return item.indexOf('expires') === -1 ? item.replace(',', '\n') : item;
            })
            .join(';')
            .split('\n')
            .map((item: string) => {
                return parse(item.split(';')[0]);
            })
            .forEach((item: Object) => {
                cookies = {
                    ...cookies,
                    ...item,
                };
            });

        this.addCookies(cookies);
        return response;
    };

    /**
     * Get cookies as human readable object.
     */
    public getCookies(): {[cookie: string]: string} | null {
        const cookies = this.getDefaultHeaders().cookie;
        if (typeof cookies !== 'string') {
            return null;
        }

        return parse(cookies);
    }

    /**
     * Add object of cookies.
     *
     * @param cookies - Object of cookies
     */
    public addCookies(cookies: {[cookie: string]: string}) {
        this.setDefaultHeader('cookie', CookieApi.serializeCookies({
            ...(this.getCookies()),
            ...cookies,
        }));
    }
}