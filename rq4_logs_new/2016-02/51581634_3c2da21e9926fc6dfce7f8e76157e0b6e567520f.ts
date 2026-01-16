module Spa {
    export class Rest {
        private url: string;//url to connect to
        private headers: Object;//headers to append to query
        private type: string;//data type to be sent with query
    
        private error: Function;//function to run on error with request
    
        constructor(url: string, type: string = "json") {
            this.url = url;
            this.headers = {};
            this.type = type;
            this.error = null;
        }

        setError(error: Function) {
            this.error = error;
        }

        addHeader(key: string, value: string) {
            this.headers[key] = value;
        }

        setHeader(headers: Object) {
            this.headers = headers;
        }

        getHeaders(): Object {
            return this.headers;
        }
    
        /**
          * do a GET request to given path of the global URL defined for the class
          * @param  {string}   path the path which to do a get request
          * @param  {object}   data that should be sent with request
          * @param  {Function} next function to run when request is done
          */
        get(path: string, data: Object, next: Function) {
            this.request("GET", path, data, next);
        }

        /**
         * do a POST request to given path of the global URL defined for the class
         * @param  {string}   path the path which to do a get request
         * @param  {object}   data that should be sent with request
         * @param  {Function} next function to run when request is done
         */
        post(path: string, data: Object, next: Function) {
            this.request("POST", path, data, next);
        }

        /**
         * do a PUT request to given path of the global URL defined for the class
         * @param  {string}   path the path which to do a get request
         * @param  {object}   data that should be sent with request
         * @param  {Function} next function to run when request is done
         */
        put(path: string, data: Object, next: Function) {
            this.request("PUT", path, data, next);
        }

        /**
         * do a DELETE request to given path of the global URL defined for the class
         * @param  {string}   path the path which to do a get request
         * @param  {object}   data that should be sent with request
         * @param  {Function} next function to run when request is done
         */
        delete(path: string, data: Object, next: Function) {
            this.request("DELETE", path, data, next);
        }

        request(type: string, path: string, data: Object, next: Function) {
            //TODO: check if data is empty
        
            var self = this;
            $.ajax({
                url: this.url + path,
                type: type,
                data: data,
                dataType: this.type,
                contentType: "application/json",
                headers: this.headers,

                success: function(result) {
                    if (result)
                        next(result);
                    else { //an error occurred..
                        var error = {
                            code: 100,
                            message: "Error with data",
                            path: path
                        };

                        if (self.error !== null) //check if a global error function is defined
                            self.error(error);
                        else //if not, pass error message to function ´next´
                            next(null, error);
                    }
                },
                error: function(error) {
                    //generate error object from recieved error msg, to only return data of interest
                    var errorObj = {
                        code: error.status,
                        message: error.statusText,
                        path: path
                    };

                    if (self.error !== null)
                        self.error(errorObj);
                    else
                        next(null, errorObj);
                }
            });
        }
    }
}