module.exports = {
  presets: [
    [
      "@babel/preset-dev",
      {
        targets: {
          noe: "current",
        },
      },
    ],
    "@babel/preset-typescript",
    "@babel/preset-react",
  ],
};