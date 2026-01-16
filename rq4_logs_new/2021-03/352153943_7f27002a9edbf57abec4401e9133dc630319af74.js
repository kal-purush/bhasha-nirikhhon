const filterChildren = child =>
  child.name.indexOf("mini-css-extract-plugin") == -1;

const filterWarnings = warning =>
  warning.message.indexOf("[mini-css-extract-plugin]") == -1;

const DEFAULT_OPTIONS = { children: true, warnings: false };

const getOptionValue = (options, key) =>
  options[key] !== undefined ? options[key] : DEFAULT_OPTIONS[key];

class CleanupMiniCssExtractPlugin {
  constructor(options = DEFAULT_OPTIONS) {
    this._children = getOptionValue(options, "children");
    this._warnings = getOptionValue(options, "warnings");
  }

  apply(compiler) {
    compiler.hooks.done.tap("CleanupMiniCssExtractPlugin", stats => {
      if (this._children) {
        const children = stats.compilation.children;
        if (Array.isArray(children)) {
          stats.compilation.children = children.filter(filterChildren);
        }
      }

      if (this._warnings) {
        const warnings = stats.compilation.warnings;
        if (Array.isArray(warnings)) {
          stats.compilation.warnings = warnings.filter(filterWarnings);
        }
      }
    });
  }
}

module.exports = CleanupMiniCssExtractPlugin;