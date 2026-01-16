import React from 'react';

const lazyHome = React.lazy(() => import('../pages/Home/Home'));

interface Routes {
  index?: boolean;
  path: string;
  component: any;
}

const publicRoutes: Routes[] = [];

const privateRoutes: Routes[] = [{ path: '/home', component: lazyHome }];

export { publicRoutes, privateRoutes };