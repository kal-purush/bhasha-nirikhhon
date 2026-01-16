/**
 * Returns a card numeric value according to it's value
 * @param {string} value Card value
 * @returns
 */
function getCardValue(value) {
	switch (value) {
		case 'KING':
		case 'QUEEN':
		case 'JACK':
		case '0':
			return 10;
		case 'ACE':
			return 0;
		default:
			return Number.parseInt(value);
	}
}

describe('getCardValue', () => {
	test('Special cards should return 10', () => {
		expect(getCardValue('KING')).toEqual(10);
		expect(getCardValue('QUEEN')).toEqual(10);
		expect(getCardValue('JACK')).toEqual(10);
		expect(getCardValue('0')).toEqual(10);
	});

	test('Ace should return 0', () => {
		expect(getCardValue('ACE')).toEqual(0);
	});

	test('Other cards should return their value', () => {
		const cards = ['2', '3', '4', '5', '6', '7', '8', '9'];

		for (let card of cards) {
			expect(getCardValue(card)).toBe(Number(card));
		}
	});
});