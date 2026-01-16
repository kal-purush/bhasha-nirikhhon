import { NextComponentType, NextPageContext } from 'next';
export { useTranslation } from 'react-i18next';
import NextI18next from 'next-i18next';

export const nextI18next = new NextI18next({
    defaultLanguage: 'ru',
    otherLanguages: ['uk', 'en'],
});

export const appWithTranslation = nextI18next.appWithTranslation;
export const i18n = nextI18next.i18n;
export const withTranslation = nextI18next.withTranslation;
export type I18nPage<P = {}> = NextComponentType<
  NextPageContext,
  { namespacesRequired: string[] },
  P & { namespacesRequired: string[] }
>;