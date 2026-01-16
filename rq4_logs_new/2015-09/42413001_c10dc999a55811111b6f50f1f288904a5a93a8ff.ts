/**
 * KEdit - UI
 * @author Kali@F-List.net
 */

/// <reference path="../../lib/vue.d.ts" />
/// <reference path="module.ts" />
/// <reference path="interfaces.ts" />

module Editor {
	function inputOnChange(val: string): void{
		var event: any;
		localStorage['input'] = val;

		for (event in Stats) {
			if (Stats.hasOwnProperty(event) && Stats[event].onChange) {
				Stats[event].onChange(val);
			}
		}
		return;
	}

	export module View {
		export var Main = new Vue({
			el: '#editor',
			data: {
				stats: Stats,
				input: localStorage['input'] || ''
			}
		});

		Main.$watch('input', inputOnChange)
	}
}