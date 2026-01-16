import expect from 'expect.js';
import jsdom from 'jsdom';
import got from 'got';

const { JSDOM } = jsdom;

const url = 'https://al3xback.github.io/fmentor-single-price-mocha-expectjs/';

const getData = () => {
	return got(url)
		.then((res) => {
			const { document } = new JSDOM(res.body).window;
			return document;
		})
		.catch((err) => {
			throw new Error(err);
		});
};

describe('DOM', () => {
	beforeEach(async () => {
		try {
			const document = await getData();
			global.document = document;

			const whyUsPoints = [
				'Tutorials by industry experts',
				'Peer & expert code review',
				'Coding exercises',
				'Access to our GitHub repos',
				'Community forum',
				'Flashcard decks',
				'New videos every week',
			];
			global.whyUsPoints = whyUsPoints;
		} catch (err) {
			console.log(err);
		}
	});

	it('should have a number type of subscription price inside card price element', () => {
		const cardPriceEl = document.querySelector('.card__price');
		const subscriptionPrice = +cardPriceEl
			.querySelector('.num')
			.textContent.substring(1);

		expect(subscriptionPrice).to.be.a('number');
	});

	it("should have 30 as a number in '.card__mark' element", () => {
		const cardMarkContent =
			document.querySelector('.card__mark').textContent;

		expect(cardMarkContent).to.contain(30);
	});

	it('should have an array type of why us points data', () => {
		expect(whyUsPoints).to.be.an('array');
	});
});