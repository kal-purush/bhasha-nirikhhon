
var Controls: Object = {
	edit: UI_Control_Edit.Edit
}

function readAndRemoveAttribute(element: HTMLElement, attrName: string): string {
	var value: string = null
	if (element.hasAttribute(attrName)) {
		value = element.getAttribute(attrName)
		element.removeAttribute(attrName)
	}
	return value
}

function readControlParams(element: HTMLElement, c: any): Object {
	var params: Object = { }

	var p = c
	if (!p._params) {
		var _params = {}
		for (let i = 0, c = p.params, l = c.length; i < l; i++) {
			_params[c[i]] = true
		}
		p._params = _params
	}

	var names = p._params


	var atts = element.attributes
	for (var j = 0, k = atts.length; j < k; j++) {
		var attr = atts[j]
		if (attr) {
			var aname = attr.name
			if (aname === 'name' || aname in names) {
				params[aname] = attr.value
				element.removeAttribute(aname)
			}
		}
	}

	return params
}

export class Builder {

	static build(owner: PluginManager.Manager, element: HTMLElement): void {

	    var c = element.querySelectorAll("*")	    
	    for(var i = 0, l = c.length; i < l; i++) {
	    	var item = <HTMLElement>c[i]
			if( item.hasAttribute("control") ) {

				var className = readAndRemoveAttribute(item, "control")

				if (className in Controls) {
					let c = Controls[className]
					var control = new c(readControlParams(item, c))

					owner[control.name] = control

					control.place(item)
				}


/*
				if(controlClass in Controls) {
					var cc = Controls[controlClass]
					var p = readControlParams(item, cc.prototype.params)
					p.name = controlName
					var control = cc.create(p)
					// control.init()
					// control.name = controlName
					owner[controlName] = control
					owner.addPlugin(control)
					if(control.events) {
						for(var i1 = 0, c1 = control.events, l1 = c1.length; i1 < l1; i1++) {
							var event = c1[i1]
							var method = 'on_' + controlName + '_' + event
							if(method in owner) {
								control.on(event, owner[method].bind(owner))
							}
						}
					}

					control.place(item)
				}
*/				
			}


	    }

	}

} 