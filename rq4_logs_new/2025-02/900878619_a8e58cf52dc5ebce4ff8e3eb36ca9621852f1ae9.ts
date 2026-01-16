import { format } from 'date-fns';
import { de, enUS as en, es, ptBR as pt } from 'date-fns/locale';

import type { LanguageOption } from '@/translation-manager';

function getLocale(language: LanguageOption) {
  switch (language) {
    case 'en':
      return en;
    case 'de':
      return de;
    case 'es':
      return es;
    case 'pt':
      return pt;
  }
}

function makeDateFormatter(language: LanguageOption) {
  return function (date: number | string | Date, dateStr: string) {
    return format(date, dateStr, { locale: getLocale(language) });
  };
}

export { makeDateFormatter };