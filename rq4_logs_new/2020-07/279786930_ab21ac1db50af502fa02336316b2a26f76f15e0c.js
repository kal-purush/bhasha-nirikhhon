import http from './http.js'
export const updata = () => {
	// #ifdef APP-PLUS
	plus.runtime.getProperty(plus.runtime.appid, function(widgetInfo) {
		console.log(widgetInfo)
	})
	// #endif
}