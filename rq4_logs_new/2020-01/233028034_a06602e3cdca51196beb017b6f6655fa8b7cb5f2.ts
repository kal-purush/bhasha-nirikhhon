import { useRouter } from 'next/router';
import { useMemo } from 'react';
import allRoutesData from '../../utils/data.route.json';
import { RouteData, NestedRouteData } from '../../interfaces/interface.commons';

const getRouteData = (
  routeDataObj: NestedRouteData,
  names: string[]
): RouteData => {
  const name = names[0];
  if (!name) return routeDataObj as RouteData;
  const trimmedName = names.slice(1);
  return getRouteData(routeDataObj[name], trimmedName);
};

const useRouteData = (): RouteData | null => {
  const router = useRouter();

  const routeData = useMemo(() => {
    const path = router.pathname;
    if (!path) return null;
    const paths = path.substring(1, path.length).split('/');
    return getRouteData(allRoutesData, paths);
  }, [router.pathname]);

  return routeData;
};

export default useRouteData;