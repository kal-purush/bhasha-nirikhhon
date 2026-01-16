import { defineUserConfig } from 'vuepress';
const { searchPlugin } = require('@vuepress/plugin-search');
import { defaultTheme } from '@vuepress/theme-default';

const sidebar = {
  component: [
    {
      text: '组件开发',
      children: ['/README.md'],
    },
  ],
};

export default defineUserConfig({
  lang: 'zh-CN',
  title: 'AppBuilder 组件规范',
  description: '',
  base: '/OneSIS-AppBuilder/docs/',
  theme: defaultTheme({
    logo: './favicon.png',
    docsDir: 'src',
    sidebarDepth: 2,
    sidebar: {
      '/': sidebar.component,
    },
    lastUpdated: false,
    contributors: false,
  }),

  plugins: [searchPlugin()],
});