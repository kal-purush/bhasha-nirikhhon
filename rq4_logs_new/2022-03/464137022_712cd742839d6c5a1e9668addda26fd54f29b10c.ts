import { registerPlugin, HandledRoute } from '@scullyio/scully';

const Plugin = async (routes: HandledRoute[], config = {}): Promise<HandledRoute[]> => {
  routes.map((route) => {
    if (route.type !== 'contentFolder') return route;

    // Remove strange extension (".html") added by esa from the url
    if (route.templateFile?.match(/\.html\.md$/)) {
      route.route = route.route.replace(/\.html$/, '');
    }

    return route;
  });

  return routes;
};

registerPlugin('routeProcess', 'esaRouteProcess', Plugin, 100);