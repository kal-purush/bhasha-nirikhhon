import JSData = require('js-data');
import DSHttpAdapter = require('js-data-http');
import qs = require('qs');
import JSDataPromise = JSData.JSDataPromise;
import DSHttpAdapterPromiseResolveType = JSData.DSHttpAdapterPromiseResolveType;
require('whatwg-fetch');

export const API_URL = 'https://strizhapi.herokuapp.com/api/v1/';


export class HttpAdapter extends DSHttpAdapter {
	HTTP(options?:Object):JSDataPromise<DSHttpAdapterPromiseResolveType> {
		console.log(options);
		//const {params, url} = options;
		//if (params) {
		//	url = buildUrl(url, params);
		//	delete options['params'];
		//}
		//return fetch(options) as any;
		return Promise.resolve() as any;
	}

	get(url:string, query:any) {
		url = buildUrl(API_URL + url, query);
		return this.GET(url).then(deserialize);
	}

	post(url:string, data:any) {
		return this.POST(API_URL + url, data).then(deserialize)
	}

	put(url:string, data:any) {
		return this.PUT(API_URL + url, data).then(deserialize)
	}

	del(url:string) {
		return this.DEL(API_URL + url).then(deserialize);
	}
}

function deserialize(data:any):any {
	return data.data.data;
}

function buildUrl(url:string, query:Object):string {
	if (url.substr(-1) !== '?') {
		url += '?';
	}
	if (query) {
		url += qs.stringify(query);
	}
	return url;
}