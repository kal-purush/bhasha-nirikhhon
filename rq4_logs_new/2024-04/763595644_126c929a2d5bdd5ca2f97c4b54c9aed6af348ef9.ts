import { useRouter } from 'next/router';
import { useTranslation } from 'next-i18next';

import { getQuery } from './Breadcrumbs.utils';
import type { Breadcrumb, BreadcrumbsProps } from './Breadcrumbs.types';

export function useBreadcrumbs({
  currentPageTitle,
  currentBreadcrumbLabel = currentPageTitle,
  onCatchAllRoutesTransform,
}: BreadcrumbsProps) {
  const { t } = useTranslation();
  const { pathname, query } = useRouter();

  return pathname
    .split('/')
    .slice(1)
    .reduce<Breadcrumb[]>((result, url, i, arr) => {
      const isLast = i === arr.length - 1;
      const matched = getQuery(query, url);

      if (matched) {
        const { key, value } = matched;
        const breadcrumbs = onCatchAllRoutesTransform?.(key, value) || [];

        result.push(
          ...(Array.isArray(breadcrumbs) ? breadcrumbs : [breadcrumbs])
        );
      } else {
        const paths = arr.slice(0, i + 1);

        result.push({
          href: `/${paths.join('/')}`,
          label: isLast
            ? currentBreadcrumbLabel
            : t(`ttl-breadcrumbs.${paths.join('.')}.label`),
        });
      }

      return result;
    }, []);
}