import { IPaginationQuery } from '@/types';

function addQueryParams(url: string, params: IPaginationQuery, query?: string): string {
	if (!url) return '';

	const queryString = Object.keys(params)
		.filter((key) => params[key] !== undefined && params[key] !== null && params[key] !== '')
		.map((key) => `${encodeURIComponent(key)}=${encodeURIComponent(String(params[key]))}`)
		.join('&');

	if (queryString) {
		if (query) return `${url}?${query}&${queryString}`;
		return `${url}?${queryString}`;
	}

	if (query) {
		return `${url}?${query}`;
	}
	return url;
}

export default addQueryParams;