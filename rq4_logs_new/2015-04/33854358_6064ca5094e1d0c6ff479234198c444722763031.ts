module markdown {
    export interface Client {
        /**
         * Compiles markdown string to HTML
         */
        toHTML(source : string, callback : Callback);
    }

    export interface Calllback {
        /**
         * This method will be called when execution is succeeded.
         */
        success : (out : string, limit : number) => void;

        /**
         * This method will be called when execution is failed.
         */
        error : (status : number, message : string) => void;
    }
}