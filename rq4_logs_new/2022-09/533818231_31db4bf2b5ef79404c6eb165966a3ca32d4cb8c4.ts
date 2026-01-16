import {ValidationError} from 'express-validator';
import { CustomErrors } from './custom-errors';


export class RequestValidationError extends CustomErrors{


    statusCode = 400;

    constructor(public errors:ValidationError[]){

        super('invalid request made')

        // Only because we are extending a built in class


        Object.setPrototypeOf(this, RequestValidationError.prototype)

    }

    serializeErrors(){

        return this.errors.map((err) => {

            return {message: err.msg,field: err.param}
        })
    }
}