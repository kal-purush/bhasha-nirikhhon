import { Controller } from "./Controller";
import {__DEBUG} from "../Core/Debug";

/**
 * Annotate a controller with some properties
 *
 * Supported properties:
 *      - Path can be used as a global prefix for each controller routes
 *
 * @param parameters
 * @returns {function(any): undefined}
 */
export function controller(parameters = null)
{
    return function (constructor) {
        //Ensure the decorator object is set
        if (constructor.prototype.decorate == null) {
            constructor.prototype.decorate = {}
        }

        //Merge all parametters into the decorator
        if (parameters) {
            for (var name in parameters) {
                constructor.prototype.decorate[name] = parameters[name];
            }
        }
    }
}