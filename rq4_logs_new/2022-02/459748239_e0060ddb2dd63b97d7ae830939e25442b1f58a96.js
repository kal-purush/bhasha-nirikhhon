SystemJS.config({
  baseURL:'https://unpkg.com/',
  defaultExtension: true,
  packages: {
    ".": {
      main: './main.js',
      defaultExtension: 'js'
    }
  },
  meta: {
    '*.js': {
      'babelOptions': {
         react:true
      }
    }
  },
  map: {
    'plugin-babel': 'systemjs-plugin-babel@latest/plugin-babel.js',
    'systemjs-babel-build': 'systemjs-plugin-babel@latest/systemjs-babel-browser.js',
    'react':'react@17/umd/react.production.min.js',
    'react-dom':'react-dom@17/umd/react-dom.production.min.js',
    'react-router':'react-router-dom@5.0.0/umd/react-router-dom.min.js'
  },
  transpiler: 'plugin-babel'
});

SystemJS.import('./main')
  .catch(console.error.bind(console));