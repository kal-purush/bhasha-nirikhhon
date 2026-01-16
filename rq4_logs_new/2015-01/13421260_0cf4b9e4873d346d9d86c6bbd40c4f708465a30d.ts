/// <reference path="HttpClient.ts" />

/// <reference path="kii/gas/GASClient.ts" />
module Kii {
    export class KiiContext {
        appId : string;
        appKey : string;
        url : string;
        token : string;
	deviceId : string;	
	clientFactory : () => HttpClient;

        constructor(appId : string, appKey : string, url : string) {
            this.appId = appId;
            this.appKey = appKey;
	    this.url = url;
	    
	    this.clientFactory = () => {
		return new gas.GASClient();
	    };
        }

        getAppId() : string {
	    return this.appId;
        }

	getAppKey() : string {
	    return this.appKey;
	}

	getServerUrl() : string {
	    return this.url;
	}

	setAccessToken(value : string) {
	    this.token = value;
	}

	getAccessToken() : string {
	    return this.token;
	}

	setDeviceId(value : string) {
	    this.deviceId = value;
	}

	getDeviceId() : string {
	    return this.deviceId;
	}	

	setClientFactory(factory : () => HttpClient) {
	    this.clientFactory = factory;
	}

	getNewClient() : HttpClient {
            return this.clientFactory();
	}      
  }
}