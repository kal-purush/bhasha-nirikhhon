import {Component} from "./component/Component";

let idCounter: number = -1;

export class GameObject {

	private components: { [key: string]: Component; };
	private id: number;

	constructor() {
		this.components = {};
		this.id = idCounter++;
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

	get ID (): number {
		return this.id;
	}

	delete() {
        // ok =(
	}
}