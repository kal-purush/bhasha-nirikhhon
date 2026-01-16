/// <reference path="../../typings/es6-promise/es6-promise.d.ts" />

import request = require('../utils/request');
import pu = require('../utils/promise');
import ui = require('../ui/helper');

interface IGeoIP {
	country_code: string;
	country_name: string;
	region_code: string;
	region_name: string;
	city: string;
	zip_code: string;
	time_zone: string;
	latitude: number;
	longitude: number;
	metro_code: number;
}

function formatGeoInfo(info: IGeoIP) {
	var data = [info.city, info.country_code];
	return data.filter(_ => !!_).join(', ');
}

function isIP(text: string) {
	return text.match(/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/);
}

function getIPInfo(text: string): Promise<string> {
	if (isIP(text)) {
		var url = `https://freegeoip.net/json/${text}`;
		return pu.promiseMap(
			request(url),
			(resp) => {
				var geoInfo = <IGeoIP> resp;
				return `GeoIP: ${formatGeoInfo(geoInfo)} ${ui.getExternalLinkCode(url)}`;
			}
		);
	} else {
		return Promise.reject<string>("it is not an IP");
	}
}

export = getIPInfo;