(function() {
    'use strict';
    // this function is strict...
}());
module.exports = function(grunt) {

    // AUTO TASK LOADER + EXECUTION TIME LOG
    require('load-grunt-tasks')(grunt);
    require('time-grunt')(grunt);

    // LOAD ASSEMBLE
    grunt.loadNpmTasks('assemble');
    grunt.loadNpmTasks('grunt-aws-s3');

    //////////////////////////
    ///                      //
    /// TASK CONFIG          //
    ///                      //
    ///----------------------//
    ///                      //
    /// JSHNIT               //
    /// CONNECT              //
    /// ASSEMBLE             //
    /// OPEN                 //
    /// LESS                 //
    /// CLEAN                //
    /// SED                  //
    /// SYNC                 //
    /// CONCAT               //
    /// UGLIFY               //
    /// CONVERT              //
    /// VERSION              //
    /// WEBFONT              //
    /// WATCH                //
    /////////////////////////

    grunt.initConfig({
        //// ESTABLISH GRUNT SETUP INFO
        pkg: grunt.file.readJSON('package.json'),
        //// AWS DEPLOYMENT KEYS
        //aws: grunt.file.readJSON(".keys/.awskey"),  // find key in a .awskey file  (make sure you don't commit it + add it to gitignore)

        // CHECK JS QUALITY
        jshint: {
            options: {
                jshintrc: '.jshintrc'
            },
            all: [
                //'Gruntfile.js',
                'assets/js/_main.js',
                'assets/js/vendor/**/*.js',
                '!assets/js/scripts.js'
            ]
        },
        // RUN LOCAL DEV SERVER
        connect: {
            'static': {
                options: {
                    hostname: '0.0.0.0',
                    port: 8001,
                    keepalive: false,
                    base: 'dist/'
                }
            }
        },
        assemble: {
            options: {
                plugins: ['assemble-contrib-permalinks'],
                permalinks: {
                    preset: 'pretty'
                },
                data: ['data/*.{json,yml}'],
                assets: '_src',
                partials: ['_templates/partials/*.hbs'],
                ext: '.html'
            },
            from_template_pages: {
                options: {
                    flatten: true,
                    layoutdir: '_templates/layouts',
                    layout: 'index.hbs'
                },
                // src: ['*.hbs'],
                //src: ['_templates/pages/*.hbs'],
                //dest: 'dist/'
                files: {
                    'dist/': ['_templates/pages/*.hbs']
                }
            },
            from_json_src: {
                options: {
                    flatten: false,
                    layout: '_templates/layouts/index.hbs',
                    site: {
                        title: 'This is my Blog'
                    },
                    pages: grunt.file.readJSON('_templates/data/pages.json')
                },
                files: {'dist/': ['src/index.hbs']}
            }
        },
        // OPEN WEBSITE IN BROWSER
        open: {
            local: {
                path: 'http://0.0.0.0:8001',
                app: 'Google Chrome'
            }
        },
        // CSS COMPILER
        less: {
            dist: {
                files: [
                    {
                        expand: true,
                        cwd: '_src/less',
                        src: 'app.less',
                        dest: '_src/css',
                        ext: '.css'
                    }
                ],
                options: {
                    compress: false,
                    yuicompress: true,
                    ieCompat: true,
                    // LESS source map
                    // To enable, set sourceMap to true and update sourceMapRootpath based on your install
                    sourceMap: true,
                    // sourceMapFilename: '_src/assets/css/app.min.css.map',
                    // sourceMapURL: 'app.min.css.map',
                    // sourceMapBasepath: 'dist',
                    // sourceMapRootpath: '/'

                    // sourceMapFilename: 'css/main.css.map', // where file is generated and located
                    sourceMapFilename: '_src/css/app.min.css.map',
                    sourceMapURL: '/assets/css/app.min.css.map',
                    sourceMapRootpath: ''
                }
            }
        },
        // DELETE DISTRIBUTION FILES
        clean: {
            dist: [
                'dist/**/*',
            ]
        },
        // SEARCH
        sed: {
            version: {
                pattern: '%VERSION%',
                replacement: '<%= pkg.version %>',
                recursive: true
            },
            mapfile: {
                path: '_src/css/app.min.css.map',
                pattern: '_src',
                replacement: '/assets',
                recursive: true
            }
        },
        // COPY SRC FILES FOR DISTRIBUTION
        sync: {
            dist: {
                files: [{
                        cwd: '_src/',
                        src: [
                            '**/*.html',
                            'css/**',
                            'fonts/**',
                            'img/**',
                            'js/*.js',
                            'less/**'
                                    // '!**/*.txt' /* but exclude txt files */
                        ],
                        dest: 'dist/assets/'
                    }],
                verbose: true
            },
            seo: {
                files: [{
                        cwd: '_src/seo',
                        src: [
                            '**', /* Include everything */
                                    // '!**/*.txt' /* but exclude txt files */
                        ],
                        dest: 'dist/'
                    }],
                verbose: true
            }
        },
        // COMBINE ALL JS
        concat: {
            options: {
                separator: '',
            },
            srcfile: {
                src: [
                    '_src/js/plugins/jquery/*.js',
                    '_src/js/plugins/*/*.js',
                    '_src/js/vendor/ele_*.js',
                    '_src/js/vendor/_*.js'
                ],
                dest: '_src/js/scripts.js',
            }
        },
        // MINIFY JS
        uglify: {
            dist: {
                files: {
                    '_src/js/scripts.min.js': [
                        '_src/js/scripts.js'
                    ]
                },
                options: {
                    // JS source map: to enable, uncomment the lines below and update sourceMappingURL based on your install
                    // sourceMap: 'assets/js/scripts.min.js.map',
                    // sourceMappingURL: '/app/themes/roots/assets/js/scripts.min.js.map'
                    compress: false,
                }
            }
        },
        // BUILD WEBFONTS
        webfont: {
            icons: {
                src: 'assets/icons/*.svg',
                dest: 'assets/fonts',
                templateOptions: {
                    baseClass: 'icon',
                    classPrefix: 'icon-',
                    mixinPrefix: 'icon-'
                },
                options: {
                    font: 'fontcustom',
                    syntax: 'bootstrap'
                }
            }
        },
        // CONVER DATA FORMATS
        convert: {
            options: {
                explicitArray: false
            },
            csv2json: {
                src: ['_templates/data/*.csv'],
                dest: '_templates/data/example.json'
            }
        },
        // DEPLOY VIA FTP
        'ftp-deploy': {
            build: {
                auth: {
                    host: 'server.com',
                    port: 21,
                    authKey: 'key1'
                },
                src: './',
                dest: '/path/to/destination/folder',
                exclusions: [
                    './**/.DS_Store',
                    './**/Thumbs.db',
                    'dist/tmp'
                ]
            }
        },
        // DEPLOY VIA S3
        aws: grunt.file.readJSON('aws-keys.json'),
        s3: {
            options: {
                accessKeyId: "<%= aws.AWSAccessKeyId %>",
                secretAccessKey: "<%= aws.AWSSecretAccessKey %>",
                bucket: "tapchat-dev",
                dryRun: false
            },
            build: {
                cwd: "dist/",
                src: "**"
            },
        },
        // DEV ENV WATCH TASKS
        watch: {
            less: {
                files: [
                    '_src/less/*.less',
                    '_src/less/breakpoints/*.less',
                    '_src/less/frontendpatterns/*.less',
                    '_src/less/frontendpatterns/reset/*.less',
                    '_src/less/frontendpatterns/ele/*.less'
                ],
                tasks: ['less', 'sed:mapfile', 'sync']
            },
            js: {
                files: [
                    '_src/js/vendor/**/*.js',
                    '_src/js/plugins/**/*.js'
                ],
                tasks: ['concat', 'uglify', 'sync']
            },
            livereload: {
                // Browser live reloading @ref :: https://github.com/gruntjs/grunt-contrib-watch#live-reloading
                options: {
                    livereload: true
                },
                files: [
                    'dist/assets/css/app.css',
                    'dist/assets/js/scripts.js',
                    'dist/**.html'
                ]
            },
            templates: {
                files: [
                    '_templates/**'
                ],
                tasks: ['assemble']
            }
        }




    });





    ///////////////////////////
    ///                      //
    /// TASK REGISTRATION    //
    ///                      //
    ///////////////////////////

    // -- DEFAULT
    grunt.registerTask('default', [
    ]);

    // -- HINTERS
    // JSHINT
    grunt.registerTask('hintjs', [
        'jshint'
    ]);

    // -- FONTS
    // WEBFONT GENERATION
    grunt.registerTask('make-fonts', [
        'webfont'
    ]);

    // -- STATIC SITE GENERATION
    grunt.registerTask('build', [
        'assemble',
        'sync'
    ]);

    // -- LOCAL DEV ENV SERVER
    grunt.registerTask('local-dev', [
        'clean',
        'less',
        'sed:mapfile',
        'uglify',
        'assemble',
        'sync:dist',
        'sync:seo',
        'open:local',
        'connect:static',
        'watch'
    ]);

    // -- DEPLOYMENT OPTIONS
    // S3 DEPLOYMENT
    grunt.registerTask('deploy', [
        'clean',
        'less',
        'concat',
        'uglify',
        'assemble',
        'sync',
        's3',
        'open:prod',
    ]);


};