import { Inject, LOCALE_ID, Pipe, PipeTransform } from '@angular/core';
import { LanguageService } from '../services/language.service';
import { first, map } from 'rxjs/operators';
import { Observable } from 'rxjs';
import { DecimalPipe } from '@angular/common';

const LOCALE_PER_LANG: { [key: string]: string } = {
  en: 'en-US',
  es: 'es-AR'
};

@Pipe({ name: 'toLocaleDecimal' })
export class ToLocaleDecimalPipe implements PipeTransform {
  private localeId;
  constructor(@Inject(LOCALE_ID) locale: string, private languageService: LanguageService) {
    this.localeId = locale;
  }

  transform(value: number | string, digitsInfo?: string): Observable<string|null> {
    return this.languageService.language$.pipe(
      map(lang => LOCALE_PER_LANG[lang] || 'en-US'),
      map(locale => new DecimalPipe(this.localeId).transform(value, digitsInfo, locale)),
      first()
    );
  }
}