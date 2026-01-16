/// <reference path='../../lib/illa/_module.ts'/>
/// <reference path='../../lib/illa/Event.ts'/>
/// <reference path='../../lib/illa/EventHandler.ts'/>
/// <reference path='../../lib/illa/Log.ts'/>

/// <reference path='../../lib/jQuery.d.ts'/>

/// <reference path='model/AppcacheModel.ts'/>

module sz {
	export class Main {
		
		private static instance = new Main();
		
		private appcacheModel: model.AppcacheModel;
		
		constructor() {
			jQuery(illa.bind(this.onDomLoaded, this));
		}
		
		onDomLoaded(): void {
			illa.Log.info('DOM loaded.');
			
			this.appcacheModel = new model.AppcacheModel();
			this.appcacheModel.addEventCallback(model.AppcacheModel.EVENT_READY, this.onAppcacheReady, this);
			this.appcacheModel.init();
		}
		
		protected onAppcacheReady(e: illa.Event): void {
			illa.Log.info('Appcache ready.');
			
			
		}
	}
}