
// use: Types.ts

export class AJAX extends PluginManager.Manager {

	static plugins: PluginManager.Manager = new PluginManager.Manager
	static Plugins: any = { }

	static addPlugin(plugin: PluginManager.Manager): void {
		AJAX.plugins.addPlugin(plugin)
	}

	public prop: Props
	public callback: Callback
	public req: any

	constructor(prop: Props, callback?: Callback) {
		super()
		this.prop = prop
		this.callback = callback

		this.addPlugin(AJAX.plugins)

		var req = this.req = new XMLHttpRequest();
		req.onreadystatechange = this.onRequest.bind(this);

		var params = {
			url: prop.url,
			method: prop.get ? 'GET' : 'POST',
			post: null
		}

		this.fireEvent('beforeAJAXRequest', this, params, prop)

		req.open(params.method, params.url, true)
		req.send(params.post);
	}

	onRequest(): void {

		var req = this.req, callback = this.callback ? this.callback : function() { }
        if (req.readyState == 4) {
			if (req.status == 200) {
				var o: Result = { answer: req.responseText }
				this.fireEvent('onAJAX200', this, o)

				if (o.skip_callback) {
					return
				}

				if (o.error) {
					callback(o.error, null)
				}
				else {
					callback(null, o)
				}
			}
			else if (req.status == 0) {
				callback(this, null)
			}
		}
	
	}

}