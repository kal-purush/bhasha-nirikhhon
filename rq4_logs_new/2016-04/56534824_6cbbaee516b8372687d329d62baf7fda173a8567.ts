import {Component} from "./component/Component";

export class GameObject {
    private components: { [key:string]: Component; };

    constructor() {
        this.components = {};
    }

    Add(component: Component) {
        this.components[component.constructor.name] = component;
    }

    Remove(component: Component) {
        delete this.components[component.constructor.name];
    }

    Get<T extends Component>(type: {new(): T; }): T {
        return <T>this.components[type.prototype.constructor.name];
    }
}