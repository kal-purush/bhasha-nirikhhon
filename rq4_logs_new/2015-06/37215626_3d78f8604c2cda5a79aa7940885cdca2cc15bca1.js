module.exports = function (grunt) {
    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),

        bowercopy: {
            foundation: {
                options: {
                    runBower: true,
                    // clean: true,
                    destPrefix: 'static/'
                },
                files: {
                    'js/foundation.js': 'foundation/js/foundation.js',
                    'js/foundation.min.js': 'foundation/js/foundation.min.js',
                    'js/fastclick.js': 'foundation/js/vendor/fastclick.js',
                    'js/jquery.cookie.js': 'foundation/js/vendor/jquery.cookie.js',
                    'js/jquery.js': 'foundation/js/vendor/jquery.js',
                    'js/modernizr.js': 'foundation/js/vendor/modernizr.js',
                    'js/placeholder.js': 'foundation/js/vendor/placeholder.js'
                }
            }
        },
        sass: {
            options: {
                includePaths: ['bower_components/foundation/scss']
            },
            dist: {
                options: {
                    outputStyle: 'compressed',
                    sourceMap: true
                },
                files: {
                    'static/css/app.css': 'static/scss/app.scss'
                }
            }
        },
        watch: {
            grunt: { files: ['Gruntfile.js'] },

            sass: {
                files: 'static/scss/**/*.scss',
                tasks: ['sass']
            }
        }
    });

    grunt.loadNpmTasks('grunt-sass');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-bowercopy');

    grunt.registerTask('default', ['sass','watch']);
};