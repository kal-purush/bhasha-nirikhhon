const fs = require('fs');
const chalk = require('chalk');
const express = require('express');
const { log } = require('../utils');
const runBuild = require('./build');
const history = require('connect-history-api-fallback');
const proxyMiddleware = require('http-proxy-middleware');
const openBrowser = require('../utils/devtools/openBrowser');

const {
  protocol, getRealPath,
} = require('../env');

const {
  choosePort,
  prepareUrls,
} = require('../utils/devtools/WebpackDevServerUtils');

const printInstructions = urls => {
  if (urls.lanUrlForTerminal) {
    console.log(`  ${chalk.bold('Local:')}    ${urls.localUrlForTerminal}`);
    console.log(`  ${chalk.bold('Network:')}  ${urls.lanUrlForTerminal}\n`);
  } else {
    console.log(`  ${urls.localUrlForTerminal}\n`);
  }
};

const runServer = opts => {
  const {
    outputDir, proxyTable, publicPath, autoOpenBrowser, callback,
  } = opts;

  const outputPath = getRealPath(outputDir);
  const SET_HOST = process.env.HOST || '0.0.0.0';
  const SET_PORT = parseInt(process.env.PORT, 10) || opts.buildServerPort;

  const createServer = (port, urls) => {
    const app = express();

    const staticFileMiddleware = express.static(outputPath);

    app.use(history({
      index: `${publicPath}index.html`,
    }));

    app.use(publicPath, staticFileMiddleware);

    app.listen(port, error => {
      if (error) {
        throw error;
      }

      log.green('\n✨ Local server is running at:\n');
      printInstructions(urls);

      if (autoOpenBrowser) {
        openBrowser(urls.localUrlForBrowser + publicPath.substr(1));
      }
      if (callback) callback();
    });

    Object.keys(proxyTable).forEach(context => {
      let options = proxyTable[context];
      if (typeof options === 'string') {
        options = { target: options };
      }
      app.use(proxyMiddleware(options.filter || context, options));
    });
  };

  choosePort(SET_HOST, SET_PORT)
    .then(port => {
      if (!port) {
        log.yellow('We have not found a port!');
      } else {
        const urls = prepareUrls(protocol, SET_HOST, port);
        if (fs.existsSync(outputPath)) {
          createServer(port, urls);
        } else {
          runBuild(opts, () => {
            createServer(port, urls);
          });
        }
      }
    });
};

module.exports = runServer;