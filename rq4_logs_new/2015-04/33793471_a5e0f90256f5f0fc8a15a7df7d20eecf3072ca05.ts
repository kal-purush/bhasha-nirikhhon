interface ICredentials {
    token: string;
    secret: string;
}

class Credentials implements ICredentials {
    token: string;
    secret: string;

    constructor(token:string, secret:string) {
        this.token = token;
        this.secret = secret;
    }
}