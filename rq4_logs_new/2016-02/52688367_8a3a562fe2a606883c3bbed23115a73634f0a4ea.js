'use strict';

module.exports = function(grunt) {
    grunt.initConfig({
        jshint: {
            files: ['Gruntfile.js.js', 'src/**/*.js', 'test/**/*.js'],
            options: {
                node: true,
                globals: {
                    /* MOCHA */
                    describe: false,
                    it: false,
                    xit: false,
                    before: false,
                    beforeEach: false,
                    after: false,
                    afterEach: false
                }
            }
        },
        watch: {
            files: ['<%= jshint.files %>'],
            tasks: ['default']
        },
        mochaTest: {  // Configure a mochaTest task
            test: {
                options: {
                    reporter: 'spec',
                    captureFile: 'results.txt', // Optionally capture the reporter output to a file
                    quiet: false, // Optionally suppress output to standard out (defaults to false)
                    clearRequireCache: false // Optionally clear the require cache before running tests (defaults to false)
                },
                src: ['test/**/*Spec.js']
            }
        }
    });

    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-mocha-test');

    grunt.registerTask('default', ['jshint', 'mochaTest']);
};