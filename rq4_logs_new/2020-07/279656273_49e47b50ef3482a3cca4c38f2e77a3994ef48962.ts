import * as MicrosoftGraph from "@microsoft/microsoft-graph-types"
const domain: string = "https://login.microsoftonline.com/38d9ba4d-85b0-4104-9e34-ba6b4f712c34/"

export class MailHelper {

    async getAccessToken(user: string, password: string): Promise<string> {
        const requestUrl = domain + "oauth2/token";

        let urlEncoded = new URLSearchParams();
        urlEncoded.append("grant_type", "password");
        urlEncoded.append("client_id", process.env.CLIENT_ID);
        urlEncoded.append("client_secret", process.env.CLIENT_SECRET);
        urlEncoded.append("resource", "https://graph.microsoft.com");
        urlEncoded.append("username", user);
        urlEncoded.append("password", password);

        let headers = new Headers();
        headers.append("Content-Type", "application/x-www-form-urlencoded");
        headers.append("Host", "login.microsoftonline.com");
        headers.append("Cache-Control", "no-cache");
        headers.append("Content-Length", urlEncoded.toString().length.toString());

        let request = new Request(
            requestUrl, {
            method: "POST",
            body: urlEncoded,
            headers: headers,
            redirect: "follow"
            }
        );

        return await fetch(request)
            .then(response => response.text())
            .then((result) => {
                let value = JSON.parse(result)["access_token"];
                return value;
            })
            .catch(() => {
                return null;
            });
    }

    sleep(ms: number) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

}