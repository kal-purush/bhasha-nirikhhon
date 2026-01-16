module.exports = function (grunt) {
    'use strict';
    // Project configuration
    grunt.initConfig({
        // Metadata
        pkg: grunt.file.readJSON('package.json'),
        // Task configuration
        concat: {
            options: {
                stripBanners: false
            },
            script: {
                src: [
                    'bower_components/jquery/dist/jquery.js',
                    'bower_components/jquery-easing-original/jquery.easing.1.3.js',
                    'bower_components/jquery-cycle/jquery.cycle.all.js',
                    'bower_components/maximage/lib/js/jquery.maximage.js',
                    'app/js/jquery.fullscreen.js',
                    'bower_components/jquery-hashchange/jquery.ba-hashchange.js',
                    'app/js/main.js',
                ],
                dest: 'dist/site.js'
            },
        },
        uglify: {
            options: {
                preserveComments: 'some',
                sourceMap: 'dist/site.min.js.map'
            },
            dist: {
                src: '<%= concat.script.dest %>',
                dest: 'dist/site.min.js'
            }
        },
        copy: {
            main: {
                files: [
                    {
                        expand: true,
                        flatten: true,
                        timestamp: true,
                        src: ['app/index.html'],
                        dest: 'dist/'
                    },
                ],
            },
            images: {
                files: [
                    {
                        expand: true,
                        timestamp: true,
                        cwd: 'app/images',
                        src: ['**'],
                        dest: 'dist/images/'
                    },
                ],
            },
        },
        clean: {
            build: ['dist/'],
            bower: ['bower_components/'],
            node_modules: ['node_modules/']
        },
        cssmin: {
            options: {
                shorthandCompacting: false,
                roundingPrecision: -1
            },
            target: {
                files: {
                    'dist/site.min.css': [
                        'bower_components/maximage/lib/css/jquery.maximage.css',
                        'app/css/styles.css'
                    ]
                }
            }
        },
        jshint: {
            options: {
                node: true,
                curly: true,
                eqeqeq: true,
                immed: true,
                latedef: true,
                newcap: true,
                noarg: true,
                sub: true,
                undef: true,
                unused: true,
                eqnull: true,
                browser: true,
                globals: { jQuery: true, "$": false },
                boss: true
            },
            gruntfile: {
                src: 'gruntfile.js'
            },
            scripts: {
                src: 'app/js/main.js'
            }
        }
    });

    // These plugins provide necessary tasks
    grunt.loadNpmTasks('grunt-contrib-concat');
    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-contrib-copy');
    grunt.loadNpmTasks('grunt-contrib-cssmin');
    grunt.loadNpmTasks('grunt-contrib-clean');

    // Default task
    grunt.registerTask('default', ['jshint', 'concat', 'uglify', 'copy', 'cssmin']);
    grunt.registerTask('tidy', ['clean:build']);
    grunt.registerTask('cleanall', ['clean']);
};