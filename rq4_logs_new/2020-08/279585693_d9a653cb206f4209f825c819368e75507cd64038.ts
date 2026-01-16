// style.utils
import { hexToRgba } from '../style.utils';
import * as stringUtils from '../strings.utils';

describe('Utils testing', () => {
	describe('Style utils testing', () => {
		test('Test hexToRgba functionality', () => {
			const mockedExtendHex = jest.spyOn(stringUtils, 'extendHex');

			const validHexA = '#c0c'; // #cc00cc;
			const validHexB = '#cc00cc';
			const resultA = hexToRgba(validHexA);
			const resultB = hexToRgba(validHexB);

			expect(mockedExtendHex).toHaveBeenCalled();
			expect(mockedExtendHex).toReturnWith('#cc00cc');

			expect(resultA).toEqual('rgba(204, 0, 204, 1)');
			expect(resultB).toEqual('rgba(204, 0, 204, 1)');

			mockedExtendHex.mockRestore();
		});

		test("Test hexToRgba throw error - 'Bad Hex'", () => {
			const bedHex = '#cc00';
			expect(() => hexToRgba(bedHex)).toThrow('Bad Hex');
		});
	});
});