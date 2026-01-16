import { I18n } from './i18n';
import { I18nContextProps } from './types';

const mockTranslationRecords: I18nContextProps = {
  en: {
    'header:main-label': 'My app',
    'header:customer-name': 'Welcome to {store}',
    'header:customer-student': 'Hi {std1}, {std2}'
  },
  it: 'Ciao',
  es: 'Adios {name}'
};

describe('I18n', () => {
  it('get translation without replacement', () => {
    const instance = new I18n('en', mockTranslationRecords);
    const value = instance.tr('header:main-label');
    expect(value).toBe('My app');
  });

  it('get translation with replacement', () => {
    const instance = new I18n('en', mockTranslationRecords);
    const value = instance.tr('header:customer-name', {
      store: 'i18n'
    });
    expect(value).toBe('Welcome to i18n');
  });

  it('get translation when lang is string without replacement', () => {
    const instance = new I18n('it', mockTranslationRecords);
    const value = instance.tr('key-ignored');
    expect(value).toBe('Ciao');
  });

  it('get translation when lang is string with replacement', () => {
    const instance = new I18n('es', mockTranslationRecords);
    const value = instance.tr('key-ignored', {
      name: 'i18n'
    });
    expect(value).toBe('Adios i18n');
  });

  it('get key when lang does not exist', () => {
    const instance = new I18n('en', {});
    const value = instance.tr('header:main-label');
    expect(value).toBe('header:main-label');
  });

  it('get key when key does not exist', () => {
    const instance = new I18n('en', mockTranslationRecords);
    const value = instance.tr('header:customer-number');
    expect(value).toBe('header:customer-number');
  });

  it('get default translation when the keys are undefined', () => {
    const instance = new I18n('en', mockTranslationRecords);
    const value = instance.tr('header:customer-student', {
      std1: undefined,
      std2: undefined
    });
    expect(value).toBe('Hi {std1}, {std2}');
  });
});