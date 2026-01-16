import { IntlHelper } from './intl-helper';

describe('IntlHelper class', () => {
  it('returns a instance of IntlHelper when calling create', () => {
    const instance = IntlHelper.create({
      testingKey: {
        enUS: 'English value',
        ptBR: 'Portuguese value',
      },
    });

    expect(instance).toBeInstanceOf(IntlHelper);
  });

  it('creates a collection that extends the global collection', () => {
    const instance = IntlHelper.create({
      globalKey: {
        enUS: 'English global value',
        ptBR: 'Portuguese global value',
      },
    });

    const localCollection = instance.makeCollection({
      localKey: {
        enUS: 'English local value',
        ptBR: 'Portuguese local value',
      },
    });

    expect(localCollection?.globalKey).toBeDefined();
    expect(localCollection?.localKey).toBeDefined();
  });

  it('creates a translation function', () => {
    const instance = IntlHelper.create({
      globalKey: {
        enUS: 'English global value',
        ptBR: 'Portuguese global value',
      },
    });

    const localCollection = instance.makeCollection({
      localKey: {
        enUS: 'English local value',
        ptBR: 'Portuguese local value',
      },
    });

    const t = instance.makeTranslator({
      collection: localCollection,
    });

    expect(t('globalKey')).toBe('English global value');
    expect(t('localKey')).toBe('English local value');
  });

  describe('translation function', () => {
    it('replaces strings using the replacer object', () => {
      const instance = IntlHelper.create({
        hello: {
          enUS: 'Hello {name}!',
          ptBR: 'Olá, {name}!',
        },
      } as const);

      const t = instance.makeTranslator({ language: 'enUS' });
      expect(t('hello', { name: 'World' })).toBe('Hello World!');
    });

    it('shows the enUS translation if the language is not found', () => {
      const instance = IntlHelper.create({
        testingKey: {
          enUS: 'English value',
        },
      });

      const t = instance.makeTranslator({ language: 'ptBR' });
      expect(t('testingKey')).toBe('English value');
    });
  });
});