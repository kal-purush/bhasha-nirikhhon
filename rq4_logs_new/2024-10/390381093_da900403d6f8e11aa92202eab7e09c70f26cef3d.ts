import type { Plugin, ResolvedConfig } from 'vite';

export interface UmamiPluginOptions {
  /**
   * The ID of the project Clarity provides to you.
   *
   * Can be found in the URL of your project.
   *
   * @example `k4vhy94oj3`
   */
  websiteId: string;

  /**
   * Host where the script is hosted
   * @default "https://cloud.umami.is"
   */
  host?: string;

  /**
   * Whether to inject the script in development mode.
   *
   * @default false
   */
  enableInDevMode?: boolean;
}

export default function umamiPlugin(options: UmamiPluginOptions): Plugin {
  let config: ResolvedConfig;
  return {
    name: 'umami-script',
    enforce: 'pre',
    configResolved(resolvedConfig) {
      config = resolvedConfig;
    },
    transformIndexHtml() {
      if (config.command === 'serve' && !options.enableInDevMode) {
        return [];
      }

      if (!options.websiteId) {
        throw new Error(
          '[umami-script] No website id provided. Please provide a website id.',
        );
      }
      const src = `${options.host ?? 'https://cloud.umami.is'}/script.js`;

      return [
        {
          tag: 'script',
          attrs: {
            defer: true,
            crossorigin: true,
            src,
            'data-website-id': options.websiteId,
          },
          children: '',
          injectTo: 'head',
        },
      ];
    },
  };
}