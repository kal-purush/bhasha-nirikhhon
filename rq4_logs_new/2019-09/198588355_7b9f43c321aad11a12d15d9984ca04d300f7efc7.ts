import { TranslateSheetRow, TranslateSheet } from '../../appscript/translateSheet';

const values = [['ID', '属性', '原文', '翻訳', '機械翻訳', 'タグ', 'コメント']];

describe(`class TranslateSheetRow`, () => {
  describe(`new TranslateSheetRow`, () => {
    it('standard', () => {
      expect(new TranslateSheetRow(values[0], 'testName', 3)).toMatchObject({
        id: 'ID',
        attribute: '属性',
        original: '原文',
        translate: '翻訳',
        tag: 'タグ',
        comments: 'コメント',
        location: {
          sheetName: 'testName',
          rowNumber: 3,
        },
      });
    });

    it('empty', () => {
      expect(new TranslateSheetRow([], 'testName', 3)).toMatchObject({
        id: '',
        attribute: '',
        original: '',
        translate: '',
        tag: '',
        comments: '',
        location: {
          sheetName: 'testName',
          rowNumber: 3,
        },
      });
    });
  });
});

describe(`class TranslateSheet`, () => {
  describe(`TranslateSheet#getName`, () => {
    it('success', () => {
      const mock = { getName: () => 'testName' };
      expect(new TranslateSheet(mock).getName()).toEqual('testName');
    });

    it('not implementation', () => {
      expect(() => new TranslateSheet({}).getName()).toThrowError();
    });
  });

  describe(`TranslateSheet#getTranslateRows`, () => {
    it('success', () => {
      const mock = { getName: () => 'testName', getRange: () => ({ getValues: () => values }) };
      expect(new TranslateSheet(mock).getTranslateRows()).toMatchObject([
        {
          id: 'ID',
          attribute: '属性',
          original: '原文',
          translate: '翻訳',
          tag: 'タグ',
          comments: 'コメント',
          location: {
            sheetName: 'testName',
            rowNumber: 3,
          },
        },
      ]);
    });

    it('getName is not implementation', () => {
      const mock = { getRange: () => ({ getValues: () => values }) };
      expect(() => new TranslateSheet(mock).getTranslateRows()).toThrowError();
    });

    it('getRange is not implementation', () => {
      const mock = { getName: () => 'testName' };
      expect(() => new TranslateSheet(mock).getTranslateRows()).toThrowError();
    });
  });

  describe(`TranslateSheet#getName`, () => {
    it('success', () => {
      const mock = { getTabColor: () => '#ffffff' };
      expect(new TranslateSheet(mock).getTabColor()).toEqual('#ffffff');
    });

    it('not implementation', () => {
      expect(() => new TranslateSheet({}).getTabColor()).toThrowError();
    });
  });
});