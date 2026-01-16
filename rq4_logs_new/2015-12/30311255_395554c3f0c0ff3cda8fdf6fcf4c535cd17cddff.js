module.exports = function(karma) {
    karma.set({
        browsers : ['Chrome'],

        // include browserify first in used frameworks
        frameworks: [ 'browserify', 'jasmine'],

        //// add all your files here,
        //// including non-commonJS files you need to load before your test cases
        files: [
            'app/boot.js',
            'test/**/*.js'
        ],

        // add preprocessor to the files that should be
        // processed via browserify
        preprocessors: {
            'app/boot.js': [ 'browserify' ],
            'test/**/*.js': [ 'browserify' ]
        },

        // see what is going on
        logLevel: 'LOG_DEBUG',

        // use autoWatch=true for quick and easy test re-execution once files change
        autoWatch: true,

        // add additional browserify configuration properties here
        // such as transform and/or debug=true to generate source maps
        browserify: {
            debug: true,
            transform: [
                ['babelify']
            ]
        }
    });
};