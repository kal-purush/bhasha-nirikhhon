module.exports = function (api) {
  api.cache(true);
  return {
    presets: ["@babel/preset-react", "minify"],
    plugins: ["@babel/plugin-transform-react-jsx-source"],
  };
};