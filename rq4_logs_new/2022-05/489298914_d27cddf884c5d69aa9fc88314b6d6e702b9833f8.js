// These are the Eleventy settings
// You shouldn't need to change anything

module.exports = function (eleventyConfig) {
    eleventyConfig.addPassthroughCopy("src/assets/css");
    eleventyConfig.addPassthroughCopy("src/assets/js");
    return {
        dir: {
            input: "src",
            includes: "_includes",
            // put the layouts in their own folder
            // layouts: "_layouts",
            //change where the website it output
            output: "final-website",
        }
    };
};