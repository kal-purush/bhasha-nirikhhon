/**
 * Created by anderson.mota on 19/02/2015.
 */
module.exports = function(grunt) {

    require('load-grunt-tasks')(grunt);

    var base_path = "./";

    var components = require('wiredep')();
    var componentsJS = components.js;
    var componentsCSS = components.css;
    var componentsSCSS = components.scss;

    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),
        compass: {
            dist: {
                options: {
                    config: 'config.rb'
                }
            }
        },
        cssmin: {
            my_target: {
                files: [{
                    expand: true,
                    cwd: base_path + 'css/',
                    src: ['*.css', '!*.min.css', componentsCSS],
                    dest: base_path + 'css/',
                    ext: '.min.css'
                }]
            }
        },
        uglify: {
            options: {
                banner: '/*! <%= pkg.name %> <%= grunt.template.today("dd-mm-yyyy") %> */\n'
            },
            dist: {
                files: {
                    'js/app.min.js': [
                        componentsJS,
                        base_path + 'js/**/*.js',
                        '!./**/*.min.js'
                    ]
                }
            }
        },
        concurrent: {
            dev: {
                tasks: ['watch:scripts', 'watch:style'],
                options: {
                    logConcurrentOutput: true
                }
            }
        },
        watch: {
            scripts: {
                files: [base_path + 'js/**/*.js', './Gruntfile.js', '!./**/*.min.js'],
                tasks: ['uglify']
            },
            style: {
                files: [base_path + 'sass/**/*.scss'],
                tasks: ['style']
            }
        }
    });

    grunt.registerTask('style', ['compass', 'cssmin']);
    grunt.registerTask('default', ['uglify', 'style']);
    grunt.registerTask('develop', ['uglify', 'style', 'concurrent:dev']);
};