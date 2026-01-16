module.exports = function(grunt) {
    require('load-grunt-tasks')(grunt);
    grunt.initConfig({
        //VARIABLES <%= path %>
        //path to main css file
        pathCSS: 'css/main.css',
        //path to main scss file
        pathSCSS: 'scss/main.scss',
        //path to main js file
        pathJS: 'js/myjs.js',

        pkg: grunt.file.readJSON('package.json'),
        watch: {
            scss: {
                files: '<%= pathSCSS %>',
                tasks: ['sass']
            },
            js: {
                files: '<%= pathJS %>',
                tasks: ['newer:uglify:all']
            }
        },
        cssbeautifier: {
            files: ["css/*.css", '!css/bootstrap.css'],
            options: {
                indent: '  ',
                openbrace: 'end-of-line',
                autosemicolon: false
            }
        },
        uglify: {
            options: {
                sourceMap: false
            },
            files: {
                'js/myjs.min.js': ['<%= pathSCSS %>']
            }
        },
        autoprefixer: {
            options: {
                safe: true
            },
            single_file: {
                src: 'css/*.css',
                expand: true,
                flatten: true,
                src: 'css/*.css',
                dest: 'css/'
            }
        },
        "jsbeautifier": {
            files: ["js/*.js", "Gruntfile.js"],
            options: {}
        },
        sass: {
            options: {
                sourceMap: true
            },
            dist: {
                files: {
                    '<%= pathCSS %>': '<%= pathSCSS %>'
                }
            }

        },
        cmq: {
            options: {
                log: false
            },
            your_target: {
                files: {
                    '<%= pathCSS %>': ['<%= pathCSS %>']
                }
            }
        },
        cssmin: {
            options: {
                shorthandCompacting: false,
                roundingPrecision: -1
            },
            target: {
                files: [{
                    expand: true,
                    cwd: 'css',
                    src: ['css/*.css', '!css/*.min.css'],
                    dest: 'css',
                    ext: '.min.css'
                }]
            }
        },
        prettysass: {
            options: {
                alphabetize: true
            },
            app: {
                src: ['scss/*.scss']
            },
        }
    });

    grunt.loadNpmTasks('grunt-cssbeautifier');
    grunt.loadNpmTasks('grunt-autoprefixer');
    grunt.loadNpmTasks("grunt-jsbeautifier");
    grunt.loadNpmTasks('grunt-prettysass');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-contrib-cssmin');
    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-combine-media-queries');


    grunt.registerTask('clean', ['autoprefixer', 'cmq', 'cssbeautifier', 'jsbeautifier', 'prettysass']);
    grunt.registerTask('bigbrother', ['watch']);
    grunt.registerTask('build', ['autoprefixer', 'cmq', 'uglify', 'cssmin']);
};