import { registerPlugin, HandledRoute, getMyConfig } from '@scullyio/scully';
import fs from 'fs';
import path from 'path';

export interface LunrPluginOptions {
  lunr: Function;
  lang?: string;
  indexJsonOutputPath: string;
}

const Plugin = async (routes: HandledRoute[]): Promise<HandledRoute[]> => {
  const config: LunrPluginOptions = getMyConfig(Plugin);
  const lunr = config.lunr;

  const lunrIndex = lunr(function () {
    let lunr_ = this;
    lunr_.ref('route');
    lunr_.field('content');

    if (config.lang) {
      lunr_.use((lunr as any)[config.lang]);
    }

    routes.map((route) => {
      if (route.type !== 'contentFolder') return route;
      if (!route.templateFile) return route;

      const content = fs.readFileSync(route.templateFile).toString('utf-8');
      lunr_.add({
        route: route.route,
        content: content,
      });

      return route;
    });
  });

  // Save index of lunr
  const lunrIndexJsonOutputDir = path.dirname(config.indexJsonOutputPath);
  if (!fs.existsSync(lunrIndexJsonOutputDir)) {
    fs.mkdirSync(lunrIndexJsonOutputDir, { recursive: true });
  }
  fs.writeFileSync(config.indexJsonOutputPath, JSON.stringify(lunrIndex));

  // Done
  return routes;
};

registerPlugin('routeProcess', 'lunr', Plugin, 100);