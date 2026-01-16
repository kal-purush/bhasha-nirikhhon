const { formatRelative } = require('date-fns');

module.exports = function(eleventyConfig) {
  eleventyConfig.addPassthroughCopy('src/static');

  eleventyConfig.addNunjucksShortcode('timeAgo', function(ts) {
    return formatRelative(new Date(ts * 1000), new Date());
  });

  return {
    dir: {
      input: './src',
    },
  };
};