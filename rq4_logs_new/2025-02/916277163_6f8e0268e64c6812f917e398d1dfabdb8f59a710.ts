import type { StorybookConfig } from '@storybook/react-webpack5';

import { TsconfigPathsPlugin } from 'tsconfig-paths-webpack-plugin';

import * as sass from 'sass';

const currentBrand = process.env.BRAND || 'geek';

const brand = currentBrand.replace(/\s/g, '');

const config: StorybookConfig = {
  stories: ['../src/**/*.mdx', '../src/**/*.stories.@(js|jsx|mjs|ts|tsx)'],
  addons: [
    '@storybook/addon-webpack5-compiler-swc',
    '@storybook/addon-onboarding',
    '@storybook/addon-essentials',
    '@chromatic-com/storybook',
    '@storybook/addon-interactions',
  ],
  framework: {
    name: '@storybook/react-webpack5',
    options: {},
  },
  webpackFinal: async (config) => {
    // @ts-ignore
    config.resolve.plugins = [ new TsconfigPathsPlugin()];
    // @ts-ignore
    config.module.rules.push({
      test: /\.scss$/,
      use: [
        'style-loader',
        'css-loader',
        {
          loader: 'sass-loader',
          options: {
            additionalData: `
              @import "~@repo/tokens/dist/${brand}/css/_variables.css";
              @import "~@repo/tokens/dist/${brand}/scss/_variables.scss";
            `,
            implementation: sass,
          }
        }
      ]
    });
    return config;
  }
};
export default config;