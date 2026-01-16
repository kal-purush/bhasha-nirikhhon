import {
  createEdge,
  createNode,
  createGraph,
} from '../FP';
import findPath from './dijkstra';

const baseWeb = createNode<string>('WEB Technologies', 'base_web');
const html = createNode<string>('html', 'html');
const css = createNode<string>('css', 'css');
const js = createNode<string>('javascript', 'js');
const npm = createNode<string>('npm/yarn', 'npm');
const sass = createNode<string>('sass', 'sass');
const postCss = createNode<string>('post css', 'postcss');
const cssFrame = createNode<string>('semanticUI', 'semanticUI');
const bem = createNode<string>('BEM', 'bem');
const prettier = createNode<string>('Prettier', 'prettier');
const esLint = createNode<string>('ESLint', 'eslint');
const webpack = createNode<string>('Webpack', 'webpack');
const frontFrameworks = createNode<string>('Pick a Framework', 'front_framework')
const react = createNode<string>('React', 'react');
const angular = createNode<string>('Angular', 'ng');
const vue = createNode<string>('Vue.js', 'vue');
const cssInJs = createNode<string>('css in js', 'cssinjs');
const styledComponents = createNode<string>('Styled Components', 'styled_components');
const cssModules = createNode<string>('css modules', 'css_modules');
const test = createNode<string>('Testing your Apps', 'test');
const jest = createNode<string>('Jest', 'jest');
const enzyme = createNode<string>('Enzyme', 'enzyme');
const cypress = createNode<string>('Cypress', 'cypress');
const pwa = createNode<string>('Progressive Web App', 'pwa');
const storage = createNode<string>('Storage', 'storage');
const webSockets = createNode<string>('Web Sockets', 'web_sockets');
const serviceWorkers = createNode<string>('Service Workers', 'service_workers');
const notificationAPI = createNode<string>('Notification', 'notification');
const types = createNode<string>('Type Checkers', 'type_checkers');
const ts = createNode<string>('TypeScript', 'ts');
const flow = createNode<string>('Flow', 'flow');

const frontendRoadmapGraph = createGraph<string>(true);

frontendRoadmapGraph.addEdge(createEdge(baseWeb, css, 6));
frontendRoadmapGraph.addEdge(createEdge(baseWeb, html, 4));
frontendRoadmapGraph.addEdge(createEdge(baseWeb, js, 8));
frontendRoadmapGraph.addEdge(createEdge(css, sass, 4));
frontendRoadmapGraph.addEdge(createEdge(css, postCss, 4));
frontendRoadmapGraph.addEdge(createEdge(css, cssFrame, 3));
frontendRoadmapGraph.addEdge(createEdge(css, bem, 3));
frontendRoadmapGraph.addEdge(createEdge(js, npm, 4));
frontendRoadmapGraph.addEdge(createEdge(js, prettier, 3));
frontendRoadmapGraph.addEdge(createEdge(js, esLint, 4));
frontendRoadmapGraph.addEdge(createEdge(js, webpack, 7));
frontendRoadmapGraph.addEdge(createEdge(js, frontFrameworks, 1));
frontendRoadmapGraph.addEdge(createEdge(frontFrameworks, react, 5));
frontendRoadmapGraph.addEdge(createEdge(frontFrameworks, angular, 7));
frontendRoadmapGraph.addEdge(createEdge(frontFrameworks, vue, 6));
frontendRoadmapGraph.addEdge(createEdge(js, cssInJs, 1));
frontendRoadmapGraph.addEdge(createEdge(cssInJs, styledComponents, 3));
frontendRoadmapGraph.addEdge(createEdge(cssInJs, cssModules, 2));
frontendRoadmapGraph.addEdge(createEdge(js, test, 3));
frontendRoadmapGraph.addEdge(createEdge(test, jest, 7));
frontendRoadmapGraph.addEdge(createEdge(test, enzyme, 3));
frontendRoadmapGraph.addEdge(createEdge(test, cypress, 6));
frontendRoadmapGraph.addEdge(createEdge(js, pwa, 3));
frontendRoadmapGraph.addEdge(createEdge(pwa, storage, 3));
frontendRoadmapGraph.addEdge(createEdge(pwa, webSockets, 5));
frontendRoadmapGraph.addEdge(createEdge(pwa, serviceWorkers, 6));
frontendRoadmapGraph.addEdge(createEdge(pwa, notificationAPI, 2));
frontendRoadmapGraph.addEdge(createEdge(js, types, 1));
frontendRoadmapGraph.addEdge(createEdge(types, ts, 6));
frontendRoadmapGraph.addEdge(createEdge(types, flow, 5));