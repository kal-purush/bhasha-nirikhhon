import * as karma from "karma";
import * as webpack from "webpack";
import webpackConfig from "./webpack.config";

interface CustomKarmaConfig extends karma.ConfigOptions {
  webpack: webpack.Configuration;
}

let debug = false;

module.exports =  function(config: karma.Config) {
  config.set(({
    basePath: "",
    frameworks: ["mocha", "chai"],
    exclude: [],
    files: [
      "test_index.ts",
    ],
    preprocessors: {
      "test_index.ts": ["webpack"]
    },
    webpack: webpackConfig,
    reporters: ["progress"],
    webpackMiddleware: {
      noInfo: true
    },
    // Dockerfile exposes this port
    port: 9876,
    colors: true,
    logLevel: config.LOG_INFO,
    browsers: ["PhantomJS"],
    concurrency: Infinity,
    plugins: [
      "karma-mocha",
      "karma-chai",
      "karma-webpack",
      "karma-phantomjs-launcher",
      "karma-source-map-support"
    ]
  }) as CustomKarmaConfig);
};