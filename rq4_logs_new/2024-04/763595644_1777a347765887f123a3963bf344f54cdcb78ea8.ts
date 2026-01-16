import { i18n } from 'next-i18next';
import { serverSideTranslations } from 'next-i18next/serverSideTranslations';
import type { GetServerSidePropsContext } from 'next';

export async function isUserEnvStatus(
  { query, req }: GetServerSidePropsContext,
  ...target: ('tutorial' | 'nontutorial' | 'auth' | 'unauth')[]
) {
  const { cookies } = req;
  const isAuth = Boolean(cookies['token']);

  const isTutorialMode =
    query.tutorial === process.env.NEXT_PUBLIC_TUTORIAL_TOKEN;

  const status = {
    tutorial: isTutorialMode,
    nontutorial: !isTutorialMode,
    auth: isAuth,
    unauth: !isAuth,
  };

  return target.length > 0 && target.every((t) => status[t]);
}

export async function getServerSideTranslations(
  { locale }: GetServerSidePropsContext,
  ns: string[] = []
) {
  if (process.env.NODE_ENV === 'development') {
    await i18n?.reloadResources();
  }

  return await serverSideTranslations(
    locale || process.env.NEXT_PUBLIC_DEFAULT_LANGUAGE,
    ['common', ...ns]
  );
}