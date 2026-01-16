
export class Button extends UI_Control.Control {

	public type: string
	public caption: string
	public width: number

	static params: Array<string> = 'width,caption'.split(',')

	constructor(params) {
		super(params)
	}

	render(): string {
		return require('Button.html')({ id: this.elemId, width: this.width, caption: this.caption })
	}

	onClick() {
		this.fireEvent('on_button_' + this.name + '_click')
	}
}

Button.prototype.width = 100