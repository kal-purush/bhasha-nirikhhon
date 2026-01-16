/// <reference path="api.d.ts" />

module API.Client {
    'use strict';

    export class CommonResponse {

        /**
         * Status code
         */
        status: number;

        /**
         * Message
         */
        message: string;

        success: boolean;
    }

}