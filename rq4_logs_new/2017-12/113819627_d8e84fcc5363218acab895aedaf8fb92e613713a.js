/**
 * Date formatter
 * @param {Date} date eg: new Date()
 * @param {String} format eg: 'yyyy-MM-dd hh:mm:ss'
 */
export default function dateFormat(date, format = 'yyyy-MM-dd') {
	let o = {
		'M+': date.getMonth() + 1,
		'd+': date.getDate(),
		'h+': date.getHours(),
		'm+': date.getMinutes(),
		's+': date.getSeconds(),
		'q+': Math.floor((date.getMonth() + 3) / 3),
		'S': date.getMilliseconds()
	};

	if (/(y+)/.test(format)) {
		//RegExp.$1表示匹配到的第一个结果(y表示年)
		format = format.replace(RegExp.$1, (date.getFullYear() + '').substr(4 - RegExp.$1.length));
	}
	for (let k in o) {
		if (new RegExp('(' + k + ')').test(format)) {
			format = format.replace(RegExp.$1, RegExp.$1.length === 1 ? o[k] : ('00' + o[k]).substr(('' + o[k]).length));
		}
	}
	return format;
}