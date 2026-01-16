import type { StorybookConfig } from '@storybook/react-webpack5';
import path from 'path';

const config: StorybookConfig = {
    framework: '@storybook/react-webpack5',
    stories: ['../src/**/*.mdx', '../src/**/*.stories.@(js|jsx|mjs|ts|tsx)'],
    typescript: {
        reactDocgen: 'react-docgen',
    },
    addons: [
        { name: 'storybook-design-token', options: { preserveCSSVars: true } },
        '@storybook/addon-viewport',
        '@storybook/addon-controls',
        '@storybook/addon-outline',
        '@storybook/addon-links'
    ],
    webpackFinal: async (config, { configType }) => {
        config.module.rules.push({
            test: /\.scss$/,
            use: ['style-loader', 'css-loader', 'sass-loader'],
            include: path.resolve(__dirname, '../')
        })
        return config
    }
};

export default config;