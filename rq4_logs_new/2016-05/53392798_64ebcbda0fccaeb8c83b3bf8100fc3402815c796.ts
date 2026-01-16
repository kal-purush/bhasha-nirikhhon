
import {Exception} from "../../Core/Exception/Exception";

/**
 * Route class
 * Defines a single route in the application
 */
export class Route
{
    public methods: string[];
    public path: string;
    public action: string;

    getController():any {
        var components = this.action.split('@');
        if (components.length == 2) {
            return components[0];
        }

        throw new Exception("Route must follow the form Controller@Method");
    }

    getMethod():any {
        var components = this.action.split('@');
        if (components.length == 2) {
            return components[1];
        }

        throw new Exception("Route must follow the form Controller@Method");
    }
}