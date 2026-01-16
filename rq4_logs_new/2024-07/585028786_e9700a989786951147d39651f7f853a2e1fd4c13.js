const PROXY_CONFIG = [
  {
    context: ["/store/base/"],
    target: "http://localhost:4300",
    secure: false,
    changeOrigin: false,
    pathRewrite: {
      "^/store/base/": "/",
    },
  },
];

module.exports = PROXY_CONFIG;