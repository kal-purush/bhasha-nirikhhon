module.exports = function (grunt) {

    var randomHexString = function (length) {
        var length = length || 32;
        var res = "";

        for (var i = 0; i < length; i++) {
            res += (parseInt(Math.random() * 16)).toString(16);
        }

        return res;
    };

    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),

        config: {
            jsFolder: "public/javascripts",
            bundleFileName: randomHexString()
        },

        concat: {
            options: {
                // define a string to put between each file in the concatenated output
                separator: ';'
            },
            dist: {
                // the files to concatenate
                src: [
                    // Libraries
                    'bower_components/jquery/dist/jquery.js',
                    // App sources
                    '<%= config.jsFolder %>/**/*.js'
                ],
                // the location of the resulting JS file
                dest: 'public/bundles/<%= config.bundleFileName %>.js'
            }
        },
        uglify: {
            options: {
                banner: '/*! <%= pkg.name %> <%= grunt.template.today("yyyy-mm-dd") %> */\n'
            },
            dist: {
                files: {
                    "public/bundles/<%= config.bundleFileName %>.min.js":  ['<%= concat.dist.dest %>']
                }
            }
        },
        jshint: {
            all: {
                options: {
                    jshintrc: './.jshintrc'
                },
                src: [
                    'Gruntfile.js',
                    '<%= config.jsFolder %>/*/**/*.js', // by "/*/" concat + uglify generated files are excluded
                    //, 'test/**/*.js'
                ]
            }
        },
        watch: {
            files: ['<%= jshint.all.src %>'],
            tasks: ['jshint']//, 'qunit']
        },
        clean: {
            build: ['public/bundles/*.js']
        },
        "string-replace": {
            dist: {
                files: {
                    './views/layout.pug': './views/layout.pug.tpl'
                },
                options: {
                    replacements: [
                        {
                            pattern: /<bundle-goes-here>/ig,
                            replacement: "<%= config.bundleFileName %>"
                        }
                    ]
                }
            }
        }
    });

    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-contrib-concat');
    grunt.loadNpmTasks('grunt-contrib-clean');
    grunt.loadNpmTasks('grunt-string-replace');

    grunt.registerTask('test', ['jshint']);
    grunt.registerTask('full', ['clean', 'jshint', 'concat', 'uglify', 'string-replace']);
    grunt.registerTask('bundle', ['clean', 'concat', 'uglify', 'string-replace']);
    grunt.registerTask('default', ['clean', 'concat', 'uglify', 'string-replace']);
};