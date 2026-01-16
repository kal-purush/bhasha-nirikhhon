const puppeteer = require('puppeteer');
const parser = require('./parser');
const supportsTime = ['daily', 'weekly', 'monthly'];

module.exports = async (type: string, since: string): Promise<Array<Github.Trending> | string> => {
	if (since && supportsTime.indexOf(since) === -1) {
		return `since type error, use [${supportsTime.join('/')}]`;
	}

	const URL = 'https://www.github.com/trending',
		LIST_SELECTOR = '.explore-content ol li',

		broswer = await puppeteer.launch(),
		page = await broswer.newPage(),

		queryType = type ? `/${type}` : '',
		querySince = since ? `?since=${since}` : '',
		url = `${URL}${queryType}${querySince}`;

	await page.goto(url);

	const result: Array<Github.Trending> = await page.evaluate(parser, LIST_SELECTOR);

	broswer.close();

	return result;
};