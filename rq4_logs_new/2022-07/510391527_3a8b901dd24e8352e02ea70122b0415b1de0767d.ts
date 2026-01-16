import ReadConfig from '../../../infra/ReadConfig';

describe('ReadConfigの振る舞いテスト', () => {
    test('meow.txtを読み込み、内容を文字列配列にして返す', async () => {
       const result = await ReadConfig.execute('meow');

       expect(result.length).toBe(32);
    });

    test('praise.txtを読み込み、内容を文字列配列にして返す', async () => {
        const result = await ReadConfig.execute('praise');

        expect(result.length).toBe(13);
    });

    test('welcomeファイルを読み込み、内容を文字列配列にして返す', async () => {
        const result = await ReadConfig.execute('welcome');

        expect(result.length).toBe(7);
    });

    test('存在しないファイルの場合、エラー', async () => {

        const result = ReadConfig.execute('hogehoge');

        await expect(result).rejects.toThrowError();
    });
})