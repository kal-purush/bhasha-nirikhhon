export default class Field {
	constructor(f, p) {
		this.p = p
		this.f = f
	}
	setOn() {
		let self = this
		let on = {
			input(v) {
				if(self.p.name) {
					self.f.$set(self.p.value, self.p.name, v)
				}
			}
		}
		return Object.assign(on, this.p.on)
	}
	setProps() {
		return Object.assign(this.p.opts, {
			value: this.p.value ? this.p.value[this.p.name] : ''
		})
	}
	render(h, ctx) {
		let props = this.setProps()
		let on = this.setOn()
		return h(this.p.type, {
			props,
			on
		})
	}
}