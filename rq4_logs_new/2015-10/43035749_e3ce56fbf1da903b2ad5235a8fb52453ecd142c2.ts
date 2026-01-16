/// <reference path="../../typings/tsd.d.ts" />

import express = require('express');
import http_status = require('http-status');

// Store route errors in an accessible location and predefine the error codes. In a more complicated application, these
// codes would not have a 1:1 mapping and would need to be customized per controller.

module RouteErrors {
    export var ERROR_GAME_EXISTS = 'Game started';

    export var ERROR_INVALID_ACTION = 'Invalid action';
    export var ERROR_INVALID_GAME = 'Invalid game';
    export var ERROR_INVALID_MESSAGE = 'Invalid chat message';
    export var ERROR_INVALID_PLAYER = 'Invalid player';
    export var ERROR_INVALID_PLAYERNAME = 'Invalid player name';
    export var ERROR_INVALID_TURN = 'Different player turn';

    export var ERROR_MISSING_PLAYER = 'No players';


    export var StatusCodes:{[error:string]:number} = {};

    StatusCodes[ERROR_GAME_EXISTS] = http_status.BAD_REQUEST;

    StatusCodes[ERROR_INVALID_ACTION] = http_status.BAD_REQUEST;
    StatusCodes[ERROR_INVALID_GAME] = http_status.BAD_REQUEST;
    StatusCodes[ERROR_INVALID_MESSAGE] = http_status.BAD_REQUEST;
    StatusCodes[ERROR_INVALID_PLAYER] = http_status.BAD_REQUEST;
    StatusCodes[ERROR_INVALID_PLAYERNAME] = http_status.BAD_REQUEST;
    StatusCodes[ERROR_INVALID_TURN] = http_status.BAD_REQUEST;

    StatusCodes[ERROR_MISSING_PLAYER] = http_status.BAD_REQUEST;

    // Convenience method for laziness and readability.
    //
    // It is not strictly appropriate to send back raw error messages in the response
    // but for the sake of this application it is safe enough.
    export function sendErrorOrResult<T>(res:express.Response):(err:Error, result?:T)=>void {
        return (err:Error, result:T) => {
            if (err) {
                var message:string = err.message;
                var status:number = StatusCodes[message] || http_status.INTERNAL_SERVER_ERROR;

                return res.status(status).send({message: message});
            }

            res.json(result);
        }
    }
}

export = RouteErrors;