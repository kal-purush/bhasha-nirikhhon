module Wpjs {

    export class Error {
        public status: number;
        public statusText: string;
        public text: string;

        /**
        * Constructor for Error object
        * 
        * @param {Number} status
        * @param {String} statusText
        * @param {String} text
        */
        constructor(status: number, statusText: string, text: string) {
            this.status = status;
            this.statusText = statusText;
            this.text = text;
        }
    }

}