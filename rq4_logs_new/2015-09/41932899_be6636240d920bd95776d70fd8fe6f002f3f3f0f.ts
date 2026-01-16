// / < rseference path="../typings/chrome/chrome.d.ts"/>

/// <reference path="../typings/es6-promise/es6-promise.d.ts" />

/// <reference path="handlers/unixtime.ts" />
/// <reference path="handlers/twogis.ts" />
/// <reference path="tipManager.ts" />


function getResolvedPromises<T>(promises: Promise<T>[]) {
	return new Promise<T[]>((resolve, reject) => {
		var resolved: T[] = [];
		
		var completed = 0;
		var total = promises.length;
		function checkComplete() {
			completed++;
			if (completed == total) {
				resolve(resolved);
			}
		}
		
		promises.map((promise) => {
			promise.then((value) => {
				resolved.push(value);
				checkComplete();
			}, checkComplete)
		})
	})
}


var handlers: Array<(text: string) => Promise<string>> = [
	getDetectedUnixtime,
	getTwoGisFilial,
	getTwoGisRubric
];

var tipManager = new TipManager("__chrome_extension_webapi_tools_tip");

function onSelectionChange() {
	var selection = window.getSelection();
	var selectedText = selection.toString();
	if (selection.rangeCount == 1 && selectedText.length > 0) {
		getResolvedPromises(handlers.map((_) => _(selectedText)))
			.then((tips) => {
				if (tips.length) {
					tipManager.show(
						tips.join(","),
						selection.getRangeAt(0).getBoundingClientRect()
					);
				} else {
					tipManager.hide();
				}
			},
			tipManager.hide);
	} else {
		tipManager.hide();
	}
}

function isArrowKeyCode(keyCode: number) {
	return keyCode >= 37 && keyCode <= 40;
}

document.addEventListener("keyup", (ev) => {
	if (ev.shiftKey && isArrowKeyCode(ev.keyCode)) {
		onSelectionChange();
	}
});

document.addEventListener("mouseup", (ev) => {
	onSelectionChange();
});