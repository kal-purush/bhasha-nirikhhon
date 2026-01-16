module AngularTypeScript {

    export class GlobalConfiguration {
        constructor(private apiUrl: string) { }

        getApiUrl(): string {
            return this.apiUrl;
        }
    }
}