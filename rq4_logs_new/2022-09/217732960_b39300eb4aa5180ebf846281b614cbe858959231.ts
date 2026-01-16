import { onMount, setContext, type ComponentType } from 'svelte';
import navaid from './navaid';
import { flattenRoutes, type FlatRoute, type Route } from './utils';
export {default as Route } from './Route.svelte'

type RouteData = [ComponentType, unknown][];

export function useRoutes(routes: Array<Route>, base = '') {
  return useHandlers(flattenRoutes(routes), base);
}

export function useHandlers(handlers: FlatRoute[], base = '') {
  const { listen, on, route, subscribe, preload, run } = navaid<RouteData>(base);
  setContext('svelte-router-internal-handlers', { subscribe });
  handlers.forEach(r => {
    r.handlers.reverse();
    on(r.path, (params, ctx) => {
      return r.handlers.map<[ComponentType, unknown]>(([c, h]) => [
        c,
        h && h(params, ctx),
      ]);
    });
  });

  onMount(listen);

  const context = {
    route,
    run,
    preload,
  };

  setContext('svelte-router', context);
  return context;
}
