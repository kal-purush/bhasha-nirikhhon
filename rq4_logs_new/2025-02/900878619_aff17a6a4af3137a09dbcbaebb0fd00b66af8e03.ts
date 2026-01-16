import { de, enUS as en, es, ptBR as pt } from 'date-fns/locale';

import { inferLocaleFromLanguage } from './infer-locale-from-language';

describe('inferLocaleFromLanguage function', () => {
  it('should return the "en" locale for the "en" language', () => {
    const result = inferLocaleFromLanguage('en');
    expect(result).toBe(en);
  });

  it('should return the "de" locale for the "de" language', () => {
    const result = inferLocaleFromLanguage('de');
    expect(result).toBe(de);
  });

  it('should return the "es" locale for the "es" language', () => {
    const result = inferLocaleFromLanguage('es');
    expect(result).toBe(es);
  });

  it('should return the "pt" locale for the "pt" language', () => {
    const result = inferLocaleFromLanguage('pt');
    expect(result).toBe(pt);
  });

  it('should return the "pt" locale as the default case for unsupported languages', () => {
    const result = inferLocaleFromLanguage('unsupported_language' as any);
    expect(result).toBe(pt);
  });
});