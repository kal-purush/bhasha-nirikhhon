
export class Control extends PluginManager.Manager {
	 
	private static _idGenerator: number = 1

	constructor() {
		super()

		this.elemId = 'ctrl-' + Control._idGenerator ++
	}
/*
	place(selector) {
		if('string' === typeof selector) {
			selector = document.querySelector(selector)
		}		
		this.parentElement = selector		
		selector.innerHTML = this.render()

		this._afterPlace()
	}

	append(element) {
		this.parentElement = element
		this.parentElement.innerHTML = this.parentElement.innerHTML + this.render()

		this._afterPlace()
	}

	private _afterPlace() {
		this.el = document.getElementById(this.elemId)

		var c = this.el.querySelectorAll("*")
		this.processMarks([ this.el ]);
		this.processMarks(c);
		this.afterPlace(this.el);
	}
*/
	abstract afterPlace(): void;
	abstract(): string;

}
