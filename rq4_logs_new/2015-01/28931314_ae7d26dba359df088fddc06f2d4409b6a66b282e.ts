/// <reference path="../lib/jquery.d.ts"/>
/// <reference path="../lib/jquerymobile.d.ts"/>
module Wappli {
	export class Browser extends Widget {
		private container: HTMLIFrameElement;
		constructor(private getValue: () => string = null) {
			super();
			this.container = document.createElement("iframe");
			this.container.style.border = "0";
			this.container.style.width = "100%";
			this.container.style.height = "1000px";
			this.container.src = "JavaScript:''";
		}
		GetElement() {
			return this.container;
		}
		Update() {
			if (this.getValue !== null) {
				var url = this.getValue();
				if (url === null || url === undefined || url === "")
					url = "JavaScript:''";
				this.container.setAttribute("src", url);
			}
		}
	}
}