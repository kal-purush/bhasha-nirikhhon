import ExtractTargetReactionService from '../../services/ExtractReactionService/ExtractTargetReactionService';

describe('praiseパターンのとき', () => {
    const sut = new ExtractTargetReactionService('text', 'praise');

    test('対象がtextを含まないとき、空配列を返す', async () => {
        const emoji_names = [
            'meow_iine',
            'meow_bad',
            'good_good'
        ];

        const result = await sut.execute(emoji_names);

        expect(result).toEqual([]);
    })

    test('対象があるとき、対象の絵文字の名前のみ配列で返す', async () => {
        const emoji_names = [
            'text_iine',
            'text_bad',
            'text_good'
        ];

        const result = await sut.execute(emoji_names);

        expect(result).toEqual(['text_iine']);
    })

    test('対象がないとき、空配列を返す', async () => {
        const emoji_names = [
            'text_bad',
            'text_good'
        ];

        const result = await sut.execute(emoji_names);

        expect(result).toEqual([]);
    })
});