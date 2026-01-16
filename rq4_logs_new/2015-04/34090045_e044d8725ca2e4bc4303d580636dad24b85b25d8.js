// Generated on 2014-01-08 using generator-angular 0.7.1
'use strict';

// # Globbing
// for performance reasons we're only matching one level down:
// 'test/spec/{,*/}*.js'
// use this if you want to recursively match all subfolders:
// 'test/spec/**/*.js'

module.exports = function (grunt) {

    // Load grunt tasks automatically
    require('load-grunt-tasks')(grunt);

    // Time how long tasks take. Can help when optimizing build times
    require('time-grunt')(grunt);

    grunt.loadNpmTasks('grunt-requirejs');
    grunt.loadNpmTasks('grunt-contrib-requirejs');
    grunt.loadNpmTasks('grunt-contrib-compress');
    grunt.loadNpmTasks('grunt-express-server');

    // Define the configuration for all the tasks
    grunt.initConfig({

        // Project settings
        yeoman: {
            // configurable paths
            version:'0.1.0',
            app: require('./bower.json').appPath || 'hj',
            dist: 'dist',
            versionNewDir:require('./bower.json').appPath || 'dist/version/' + '<%= yeoman.version %>',
            versionPath:require('./bower.json').appPath || 'dist/version/'
        },

        express: {
            options: {
                // Override defaults here
            },
            dev: {
                options: {
                    script: 'hj_server.js'
                }
            },
            dist: {
                options: {
                    script: 'hj_dist_server.js'
                }
            },
            version: {
                options: {
                    script: 'hj_dist_version_server.js'
                }
            }
        },

        // Watches files for changes and runs tasks based on the changed files
        watch: {
            express: {
                files: ['<%= yeoman.app %>/styles/**/**'],
                tasks: ['concurrent:dist']
            },
            js: {
                files: ['<%= yeoman.app %>/js/{,*/}*.js'],
                tasks: ['newer:jshint:all'],
                options: {
                    livereload: true
                }
            },
            jsTest: {
                files: ['test/spec/{,*/}*.js'],
                tasks: ['newer:jshint:test', 'karma']
            },
            compass: {
                files: ['<%= yeoman.app %>/styles/**/**'],
                tasks: ['concurrent:dist'],
                options: {
                    livereload: true
                }
            },
            gruntfile: {
                files: ['Gruntfile.js']
            },
            livereload: {
                options: {
                    livereload: '<%= connect.options.livereload %>'
                },
                files: [
                    '<%= yeoman.app %>/{,*/}*.html',
                    '.tmp/styles/{,*/}*.css',
                    '<%= yeoman.app %>/images/{,*/}*.{png,jpg,jpeg,gif,webp,svg}'
                ]
            }
        },

        // Make sure code styles are up to par and there are no obvious mistakes
        jshint: {
            options: {
                jshintrc: '.jshintrc',
                reporter: require('jshint-stylish')
            },
            all: [
                'Gruntfile.js',
                '<%= yeoman.app %>/js/{,*/}*.js'
            ],
            test: {
                options: {
                    jshintrc: 'test/.jshintrc'
                },
                src: ['test/spec/{,*/}*.js']
            }
        },

        // Empties folders to start fresh
        clean: {
            dist: {
                files: [
                    {
                        dot: true,
                        src: [
                            '.tmp',
                            '<%= yeoman.dist %>/*',
                            '<%= yeoman.versionNewDir %>',
                            '!<%= yeoman.dist %>/version',
                            '!<%= yeoman.dist %>/.git*'
                        ]
                    }
                ]
            },
            app: {
                files: [
                    {
                        dot: true,
                        src: [
                            '.tmp',
                            '<%= yeoman.dist %>/scripts/app'
                        ]
                    }
                ]
            },
            server: '.tmp'
        },

        // Add vendor prefixed styles
        autoprefixer: {
            options: {
                browsers: ['last 1 version']
            },
            dist: {
                files: [
                    {
                        expand: true,
                        cwd: '.tmp/styles/',
                        src: '{,*/}*.css',
                        dest: '.tmp/styles/'
                    }
                ]
            }
        },

        // Compiles Sass to CSS and generates necessary files if requested
        compass: {
            options: {
                sassDir: '<%= yeoman.app %>/styles',
                cssDir: '.tmp/styles',
                generatedImagesDir: '.tmp/images/generated',
                imagesDir: '<%= yeoman.app %>/images',
                fontsDir: '<%= yeoman.app %>/styles/fonts',
                relativeAssets: false,
                assetCacheBuster: false
            },
            dist: {
                options: {
                    generatedImagesDir: '<%= yeoman.dist %>/images/generated'
                }
            },
            server: {
                options: {
                    line_comments: false,
                    lineComments: false,
                    debugInfo: false,
                    noLineComments: true
                }
            }
        },

//        requirejs: {
//            optim: {
//                options: {
//                    removeCombined: true,
//                    almond: true,
//                    appDir: '<%= yeoman.app %>/',
//                    baseUrl: 'scripts/',
//                    dir: '<%= yeoman.dist %>/',
////                    replaceRequireScript: [
////                        {
////                            files: ['<%= yeoman.dist %>/views/index.html'],
////                            module: '../page1'
////                        }
////                    ],
//                    paths: {
//                        zepto: 'lib/zepto',
//                        underscore: 'lib/underscore',
//                        backbone: 'lib/backbone',
//                        pageslider: 'lib/pageslider',
//                        fastclick:'lib/fastclick',
//                        cookie: 'lib/cookie',
//                        text: 'lib/text',
//                        iOSdialogBox: 'lib/iOSdialogBox',
//                        app: 'app'
//                    },
//                    shim: {
//                        zepto: {
//                            exports: '$'
//                        },
//                        underscore: {
//                            exports: '_'
//                        },
//                        backbone: {
//                            deps: [
//                                'underscore',
//                                'zepto'
//                            ],
//                            exports: 'Backbone'
//                        },
//                        pageslider: {
//                            deps: ['zepto'],
//                            exports: 'pageslider'
//                        },
//                        fastclick: {
//                            exports: 'fastclick'
//                        },
//                        cookie: {
//                            deps: ['zepto'],
//                            exports: 'cookie'
//                        },
//                        iOSdialogBox:{
//                            exports: 'iOSdialogBox'
//                        }
//                    },
////                    paths: {
////                        app: 'app'
////                    },
//                    modules: [
//                        //First set up the common build layer.
//                        {
//                            //module names are relative to baseUrl
//                            name: 'common',
//                            //List common dependencies here. Only need to list
//                            //top level dependencies, "include" will find
//                            //nested dependencies.
//                            include: ['zepto',
//                                'underscore',
//                                'backbone',
//                                'pageslider',
//                                'fastclick',
//                                'cookie',
//                                'iOSdialogBox'
//                            ]
//                        },
//
//                        //Now set up a build layer for each page, but exclude
//                        //the common one. "exclude" will exclude
//                        //the nested, built dependencies from "common". Any
//                        //"exclude" that includes built modules should be
//                        //listed before the build layer that wants to exclude it.
//                        //"include" the appropriate "app/main*" module since by default
//                        //it will not get added to the build since it is loaded by a nested
//                        //require in the page*.js files.
//                        {
//                            //module names are relative to baseUrl/paths config
//                            name: 'page1',
//                            include: ['app/main1'],
//                            exclude: ['common']
//                        },
//
//                        {
//                            //module names are relative to baseUrl
//                            name: 'page2',
//                            include: ['app/main2'],
//                            exclude: ['common']
//                        }
//
//                    ]
//                }
//            }
//        },
//        requirejs: {
//            optim: {
//                options: {
//                    removeCombined: true,
//                    almond: true,
//                    appDir: '<%= yeoman.app %>/scripts/',
//                    baseUrl: 'lib',
//                    dir: '<%= yeoman.dist %>/scripts/',
////                    replaceRequireScript: [
////                        {
////                            files: ['<%= yeoman.dist %>/views/index.html'],
////                            module: '../page1'
////                        }
////                    ],
//                    shim: {
//                        zepto: {
//                            exports: '$'
//                        },
//                        underscore: {
//                            exports: '_'
//                        },
//                        backbone: {
//                            deps: [
//                                'underscore',
//                                'zepto'
//                            ],
//                            exports: 'Backbone'
//                        },
//                        pageslider: {
//                            deps: ['zepto'],
//                            exports: 'pageslider'
//                        },
//                        fastclick: {
//                            exports: 'fastclick'
//                        },
//                        cookie: {
//                            deps: ['zepto'],
//                            exports: 'cookie'
//                        },
//                        iOSdialogBox:{
//                            exports: 'iOSdialogBox'
//                        }
//                    },
//                    paths: {
//                        app: '../app'
//                    },
//                    modules: [
//                        //First set up the common build layer.
//                        {
//                            //module names are relative to baseUrl
//                            name: '../common',
//                            //List common dependencies here. Only need to list
//                            //top level dependencies, "include" will find
//                            //nested dependencies.
//                            include: ['zepto',
//                                'underscore',
//                                'backbone',
//                                'pageslider',
//                                'fastclick',
//                                'cookie',
//                                'iOSdialogBox'
//                            ]
//                        },
//
//                        //Now set up a build layer for each page, but exclude
//                        //the common one. "exclude" will exclude
//                        //the nested, built dependencies from "common". Any
//                        //"exclude" that includes built modules should be
//                        //listed before the build layer that wants to exclude it.
//                        //"include" the appropriate "app/main*" module since by default
//                        //it will not get added to the build since it is loaded by a nested
//                        //require in the page*.js files.
//                        {
//                            //module names are relative to baseUrl/paths config
//                            name: '../page1',
//                            include: ['app/main1'],
//                            exclude: ['../common']
//                        },
//
//                        {
//                            //module names are relative to baseUrl
//                            name: '../page2',
//                            include: ['app/main2'],
//                            exclude: ['../common']
//                        }
//
//                    ]
//                }
//            }
//        },
        requirejs: {
            optim: {
                options: {
                    removeCombined: true,
                    almond: true,
                    appDir: '<%= yeoman.app %>/scripts/',
                    baseUrl: 'lib',
                    dir: '<%= yeoman.dist %>/lib/',
//                    replaceRequireScript: [
//                        {
//                            files: ['<%= yeoman.dist %>/views/index.html'],
//                            module: '../page1'
//                        }
//                    ],
                    shim: {
                        angular: {
                            exports: 'angular'
                        },
                        'angular-route': {
                            exports: 'angular-route'
                        },
                        'angular-resource': {
                            deps: [
                                'angular',
                                'angular-resource'
                            ],
                            exports: 'Backbone'
                        },
                        'angular-animate': {
                            deps: ['angular'],
                            exports: 'angular-animate'
                        },
                        'angular-sanitize': {
                            deps: ['angular'],
                            exports: 'angular-sanitize'
                        },
                        'angular-ui-router': {
                            deps: ['angular'],
                            exports: 'angular-ui-router'
                        },
                        ngStorage:{
                            deps: ['angular'],
                            exports: 'ngStorage'
                        }
                    },
                    paths: {
                        app: '../app'
                    },
                    modules: [
                        //First set up the common build layer.
                        {
                            //module names are relative to baseUrl
                            name: 'angular-hj',
                            //List common dependencies here. Only need to list
                            //top level dependencies, "include" will find
                            //nested dependencies.
                            include: ['angular',
                                'angular-route',
                                'angular-resource',
                                'angular-animate',
                                'angular-sanitize',
                                'angular-ui-router',
                                'ngStorage'
                            ]
                        }

                        //Now set up a build layer for each page, but exclude
                        //the common one. "exclude" will exclude
                        //the nested, built dependencies from "common". Any
                        //"exclude" that includes built modules should be
                        //listed before the build layer that wants to exclude it.
                        //"include" the appropriate "app/main*" module since by default
                        //it will not get added to the build since it is loaded by a nested
                        //require in the page*.js files.


                    ]
                }
            }
        },

        // Renames files for browser caching purposes
        rev: {
            dist: {
                files: {
                    src: [
                        '<%= yeoman.dist %>/newmall/{,*/}*.js'
                    ]
                }
            }
        },

        // Reads HTML for usemin blocks to enable smart builds that automatically
        // concat, minify and revision files. Creates configurations in memory so
        // additional tasks can operate on them
        useminPrepare: {
            options: {
                dest: '<%= yeoman.dist %>'
            },
            html: ['<%= yeoman.dist %>/views/*.html']
        },

        // Performs rewrites based on rev and the useminPrepare configuration


        usemin: {
            options: {
                assetsDirs: ['<%= yeoman.dist %>/scripts/tpl']
            },
            html: ['<%= yeoman.dist %>/scripts/tpl/{,*/}*.html'],
            css: ['<%= yeoman.dist %>/styles/{,*/}*.css']
        },
        cssmin: {
            dist: {
                files: {
                    '<%= yeoman.dist %>/styles/main.css': [
                        '.tmp/styles/main.css',
                        '<%= yeoman.app %>/styles/main.css'
                    ]
                }
            }
        },
        // The following *-min tasks produce minified files in the dist folder
        imagemin: {
            dist: {
                files: [
                    {
                        expand: true,
                        cwd: '<%= yeoman.app %>/images',
                        src: '{,*/}*.{png,jpg,jpeg,gif,webp,svg,css}',
                        dest: '<%= yeoman.dist %>/images'
                    }
                ]
            }
        },
        svgmin: {
            dist: {
                files: [
                    {
                        expand: true,
                        cwd: '<%= yeoman.app %>/images',
                        src: '{,*/}*.svg',
                        dest: '<%= yeoman.dist %>/images'
                    }
                ]
            }
        },
        htmlmin: {
            dist: {
                options: {
                    collapseWhitespace: true,
                    collapseBooleanAttributes: true,
                    removeCommentsFromCDATA: true,
                    removeOptionalTags: true
                },
                files: [
                    {
                        expand: true,
                        cwd: '<%= yeoman.dist %>/scripts',
                        src: ['*.html', 'tpl/{,*/}*.html'],
                        dest: '<%= yeoman.dist %>/scripts'
                    }
                ]
            }
        },

        // Allow the use of non-minsafe AngularJS files. Automatically makes it
        // minsafe compatible so Uglify does not destroy the ng references
//    ngmin: {
//      dist: {
//        files: [{
//          expand: true,
//          cwd: '.tmp/concat/scripts',
//          src: '*.js',
//          dest: '.tmp/concat/scripts'
//        }]
//      }
//    },
        modernizr: {
            devFile: 'bower_components/modernizr/modernizr.js',
            outputFile: '<%= yeoman.dist %>/scripts/vendor/modernizr.js',
            uglify: true
        },
        // Copies remaining files to places other tasks can use
        copy: {
            dist: {
                files: [
                    {
                        expand: true,
                        dot: true,
                        cwd: '<%= yeoman.app %>',
                        dest: '<%= yeoman.dist %>',
                        src: [
                            '*.{ico,png,txt}',
                            '.htaccess',
                            '*.html',
                            'lib/**/**',
//            'images/font-awesome/css/*.{png,jpg,jpeg,gif,webp,svg,css}',
                            'i18n/*',
                            'json/*',
                            'assets/**/**',
//                            'scripts/**/**',
                            'views/**/**',
                            'styles/**/**',
                            'bower_components/**/**',
                            'hj_server.js'
                        ]
                    },
                    {
                        expand: true,
                        cwd: '.tmp/images',
                        dest: '<%= yeoman.dist %>/images',
                        src: ['generated/*']
                    }
                ]
            },
            version: {
                files: [
                    {
                        expand: true,
                        dot: true,
                        cwd: '<%= yeoman.dist %>',
                        dest: '<%= yeoman.versionNewDir %>',
                        src: [
                            '*.{ico,png,txt}',
                            '.htaccess',
                            '*.html',
                            'lib/**/**',
//            'images/font-awesome/css/*.{png,jpg,jpeg,gif,webp,svg,css}',
                            'i18n/*',
                            'json/*',
                            'assets/**/**',
                            'scripts/**/**',
                            'views/**/**',
                            'bower_components/**/**',
                            'hj_server.js',
                            'styles/**/**'
                        ]
                    },
                    {
                        expand: true,
                        cwd: '.tmp/images',
                        dest: '<%= yeoman.dist %>/images',
                        src: ['generated/*']
                    }
                ]
            },
            styles: {
                expand: true,
                dot: true,
                cwd: '<%= yeoman.app %>/styles',
                dest: '.tmp/styles/',
                src: '{,*/}*.css'
            }
        },

        // Run some tasks in parallel to speed up the build process
        concurrent: {
            server: [
                'compass:server',
                'copy:styles'
            ],
            test: [
                'compass'
            ],
            dist: [
                'compass',
                'copy:styles'
//          ,
//        'imagemin'
            ]
        },

        uglify: {
            buildall: {//任务三：按原文件结构压缩js文件夹内所有JS文件
                options: {
                    mangle: false //不混淆变量名
                },
                files: [{
                    expand: true,
                    cwd: 'dist/',
                    src: ['scripts/**/*.js'],
                    dest: 'dist/'
                }]
            }
        }

        // Test settings
    });


    grunt.registerTask('serve', function (target) {
        if (target === 'dist') {
            return grunt.task.run(['build', 'connect:dist:keepalive']);
        }

        grunt.task.run([
            'clean:server',
//            'bower-install',
            'concurrent:server',
//            'autoprefixer',
//            'express:dev',
//            'express:prod',
            'express:dev',
            'express:dist',
            'express:version',
//            'connect:livereload',
            'watch:express'
        ]);
    });

    grunt.registerTask('server', function () {
        grunt.log.warn('The `server` task has been deprecated. Use `grunt serve` to start a server.');
        grunt.task.run(['serve']);
    });


    grunt.registerTask('build', [
        'clean:dist',
//        'bower-install',
        'useminPrepare',
//        'copy:dist',
        'concurrent:dist',
        'copy:dist',
        'autoprefixer',
//        'concat',
        // 'cdnify',


        'requirejs',
        'cssmin',
//        'uglify',
//        'rev',
//        'uglify:buildall',
        'usemin',
        'htmlmin',
        'rev',
        'clean:app',
        'copy:version'
//        ,
//        'modernizr'
    ]);


    grunt.registerTask('default', [
        'newer:jshint',
        'test',
        'build'
    ]);

    grunt.loadNpmTasks('grunt-contrib-watch');
};