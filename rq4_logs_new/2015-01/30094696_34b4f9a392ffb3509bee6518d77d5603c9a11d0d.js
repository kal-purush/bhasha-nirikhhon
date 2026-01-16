module.exports = function (grunt) {
    "use strict";

    grunt.loadNpmTasks('grunt-contrib-less');
    grunt.loadNpmTasks('grunt-contrib-concat');
    grunt.loadNpmTasks('grunt-contrib-copy');
    grunt.loadNpmTasks('grunt-browserify');
    grunt.loadNpmTasks('grunt-contrib-watch');

    // All generated content is put in a different 'build' directory this doesn't need to be checked in
    grunt.initConfig({
        config : {
            appBase:'./app'
        },
        less: {
            dev: { // TODO: add mimify
                options: {
                    paths: ["assets/css"]
                },
                files: {
                    "<%=config.appBase%>/build/css/bootstrap.css": "less/bootstrap.less"
                }
            }
        },
        browserify: {
            dist: {
                options: { // TODO: add mimify/source maps
                    transform: ['browserify-shim']
                },
                files: {
                    '<%=config.appBase%>/build/js/app.js': ['client/*.js', 'node_modules/angular/angular.js', 'node_modules/jquery/dist/jquery.js','node_modules/bootstrap/dist/js/bootstrap.js']
                }
            }
        },
        copy: {
            fonts: {
                files: [
                    {expand: true, cwd: 'node_modules/bootstrap/fonts',src: ['**'], dest: '<%=config.appBase%>/build/fonts/'}
                ]
            }
        },
        watch: {
            app: {
                files: ['client/**.js'],
                tasks:['generate']
            }
        },
        'http-server': {
            demo: {
                root:'./app',
                host:'127.0.0.1',
                autoIndex: true,
                ext: 'html'
            }
        }
    });
    grunt.loadNpmTasks('grunt-http-server');
    grunt.registerTask('generate', ['less','copy','browserify']);
    grunt.registerTask('default', 'generate');
};