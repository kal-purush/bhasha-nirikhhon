
export abstract class Dialog extends PluginManager.Manager {

	public el: HTMLElement
	public inner: HTMLElement

	open() {
		
		var el = this.el = <HTMLElement>document.createElement('div')
		el.className = 'ctrl-dialog'
		el.innerHTML = require('Dialog.html')()
		document.querySelector('body').appendChild(el)

		var inner = this.inner = <HTMLElement>el.querySelector('.inner')
		this.installElements(inner)

		// spawn controls
		// Builder.build(this, inner)

		inner.style.width = (<HTMLElement>inner.childNodes[0]).offsetWidth + 'px'
		inner.style.margin = ((el.offsetHeight - (<HTMLElement>inner.childNodes[0]).offsetHeight - 20) / 2) + 'px auto'

		inner.style.visibility = 'visible'	
	}

	abstract installElements(el: HTMLElement): void;

	close() {
		document.querySelector('body').removeChild(this.el)
	}
} 