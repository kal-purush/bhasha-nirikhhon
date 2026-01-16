declare module "httprequest" {
    interface RequestCallback {
        (error: Error, response?: any): void;
    }
    class NetworkError implements Error {
        name: string;
        message: string;
        constructor(method: string, url: string);
    }
    /**
    Example:
    
    new Request('POST', '/api/reservations').sendData({venue_id: 100}, (err, response) => {
      if (err) throw err;
      console.log('got response', res);
    });
    */
    class Request {
        private method;
        private url;
        xhr: XMLHttpRequest;
        headers: Array<[string, string]>;
        callback: RequestCallback;
        /**
        XMLHttpRequest doesn't expose method and url after setting them, so we need
        to keep track of them in the Request instance for error reporting purposes.
        */
        constructor(method: string, url: string, responseType?: string);
        /**
        Add a header-value pair to be set on the XMLHttpRequest once it is opened.
        */
        addHeader(header: string, value: string): Request;
        send(callback: RequestCallback): Request;
        sendJSON(object: any, callback: RequestCallback): Request;
        sendData(data: any, callback: RequestCallback): Request;
    }
}