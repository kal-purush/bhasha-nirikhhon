import { makeAutoObservable } from 'mobx';
import { FormElementInstance } from './formElements/types';

class Store {
	elements: FormElementInstance[] = [];

	constructor() {
		makeAutoObservable(this);
	}

	addElement(index: number, element: FormElementInstance) {
		this.elements.splice(index, 0, element);
	}

	removeElement(id: string) {
		this.elements = this.elements.filter((element) => element.id !== id);
	}
}

export const formBuilderStore = new Store();