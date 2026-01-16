/// <reference path='../../lib/jQuery.d.ts'/>

/// <reference path='../../lib/illa/_module.ts'/>
/// <reference path='../../lib/illa/Log.ts'/>

/// <reference path='model/Solver.ts'/>
/// <reference path='presenter/ScreenPresenter.ts'/>
/// <reference path='view/Screen.ts'/>

module kirako {
	export class Main {
		
		private static instance = new Main();
		
		private solver: model.Solver;
		private screenPresenter: presenter.ScreenPresenter;
		private screen: view.Screen;
		
		constructor() {
			jQuery(illa.bind(this.onDomLoaded, this));
		}
		
		protected onDomLoaded(): void {
			this.solver = new model.Solver();
			this.screen = new view.Screen(jQuery('body'));
			this.screenPresenter = new presenter.ScreenPresenter();
		}
		
		static getSolver() { return this.instance.solver }
		static getScreen() { return this.instance.screen }
		static getScreenPresenter() { return this.instance.screenPresenter }
	}
}