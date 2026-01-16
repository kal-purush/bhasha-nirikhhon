export class CookieService {
    static set(name: string, value: any, options: any) {

        const now = new Date();
        const time = now.getTime();
        now.setTime(time + 1000 * 120);
        options = {
            path: '/',
            ...options,
            expires: now
        };
        if (options.expires.toUTCString) {
            options.expires = options.expires.toUTCString();
        }
        let updatedCookie = encodeURIComponent(name) + "=" + JSON.stringify(value);
        for (let optionKey in options) {
            updatedCookie += "; " + optionKey;
            let optionValue = options[optionKey];
            if (optionValue !== true) {
                updatedCookie += "=" + optionValue;
            }
        }
        document.cookie = updatedCookie;
    }

    static remove(name: string) {
        this.set(name, "",{
            max_age: -1
        })
    }

    static async get(name: string) {
        let matches = document.cookie.match(new RegExp(
            "(?:^|; )" + name.replace(/([\.$?*|{}\(\)\[\]\\\/\+^])/g, '\\$1') + "=([^;]*)"
        ));
        return matches ? decodeURIComponent(matches[1]) : '';
    }

}