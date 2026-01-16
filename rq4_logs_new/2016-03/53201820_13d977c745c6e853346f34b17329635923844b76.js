(function () {

    "use strict";
    var LIVERELOAD_PORT, lrSnippet, mountFolder;

    LIVERELOAD_PORT = 35728;

    lrSnippet = require("connect-livereload")({
        port: LIVERELOAD_PORT
    });

    mountFolder = function(connect, dir) {
        return connect["static"](require("path").resolve(dir));
    };

    module.exports = function(grunt) {
        var yeomanConfig;
        require("load-grunt-tasks")(grunt);
        require("time-grunt")(grunt);

        yeomanConfig = {
            app: "app",
            dist: "dist"
        };
        try {
            yeomanConfig.app = require("./bower.json").appPath || yeomanConfig.app;
        } catch (_error) {}

        grunt.initConfig({
            yeoman: yeomanConfig,
            cnf: grunt.file.readJSON('config.json'),
            watch: {
                compass: {
                    files: ["<%= yeoman.app %>/styles/**/*.{scss,sass}"],
                    tasks: ["compass:server"]
                },
                less: {
                    files: ["<%= yeoman.app %>/styles-less/**/*.less"],
                    tasks: ["less:server"]
                },
                gruntfile: {
                    files: ['Gruntfile.js']
                },
                livereload: {
                    options: {
                        livereload: LIVERELOAD_PORT
                    },
                    files: [
                        "<%= yeoman.app %>/index.html",
                        "<%= yeoman.app %>/views/**/*.html", 
                        "<%= yeoman.app %>/styles/**/*.scss", 
                        "<%= yeoman.app %>/styles-less/**/*.less", 
                        ".tmp/styles/**/*.css", 
                        "{.tmp,<%= yeoman.app %>}/scripts/**/*.js", 
                        "<%= yeoman.app %>/images/**/*.{png,jpg,jpeg,gif,webp,svg}", 
                    ]
                }
            },
            connect: {
                options: {
                    port: 9009,
                    hostname: "localhost"
                },
                livereload: {
                    options: {
                        middleware: function(connect) {
                            return [
                                function(req, res, next) {
                                    res.setHeader('Access-Control-Allow-Origin', '*');
                                    res.setHeader('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
                                    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
                                    return next();
                                },
                                lrSnippet, 
                                mountFolder(connect, ".tmp"), 
                                mountFolder(connect, yeomanConfig.app)
                            ];
                        }
                    }
                },
                test: {
                    options: {
                        middleware: function(connect) {
                            return [mountFolder(connect, ".tmp"), mountFolder(connect, "test")];
                        }
                    }
                },
                dist: {
                    options: {
                        middleware: function(connect) {
                            return [mountFolder(connect, yeomanConfig.dist)];
                        }
                    }
                }
            },
            open: {
                server: {
                    url: "http://localhost:<%= connect.options.port %>"
                }
            },
            clean: {
                dist: {
                    files: [
                        {
                            dot: true,
                            src: [".tmp", "<%= yeoman.dist %>/*", "!<%= yeoman.dist %>/.git*"]
                        }
                    ]
                },
                all: [
                    ".tmp", ".DS_Store", ".sass-cache",
                    "readme.md"
                ],
                server: ".tmp"
            },
            jshint: {
                options: {
                    jshintrc: ".jshintrc"
                },
                all: [
                    "Gruntfile.js", 
                    "<%= yeoman.app %>/scripts/goethe/**/*.js",
                    "<%= yeoman.app %>/scripts/core/**/*.js"
                ]
            },
            compass: {
                options: {
                    sassDir: "<%= yeoman.app %>/styles",
                    cssDir: ".tmp/styles",
                    generatedImagesDir: ".tmp/styles/ui/images/",
                    imagesDir: "<%= yeoman.app %>/styles/ui/images/",
                    javascriptsDir: "<%= yeoman.app %>/scripts",
                    fontsDir: "<%= yeoman.app %>/fonts",
                    importPath: "<%= yeoman.app %>/bower_components",
                    httpImagesPath: "styles/ui/images/",
                    httpGeneratedImagesPath: "styles/ui/images/",
                    httpFontsPath: "fonts",
                    relativeAssets: true
                },
                dist: {
                    options: {
                        outputStyle: 'compressed',
                        debugInfo: false,
                        noLineComments: true,
                        sourcemap: false
                    }
                },
                server: {
                    options: {
                        noLineComments: false,
                        sourcemap: false,
                        debugInfo: true
                    }
                },
                forvalidation: {
                    options: {
                        debugInfo: false,
                        noLineComments: false
                    }
                }
            },
            less: {
                server: {
                    options: {
                        strictMath: true,
                        dumpLineNumbers: true,
                        sourceMap: true,
                        sourceMapRootpath: "",
                        outputSourceFiles: true
                    },
                    files: [
                        {
                            expand: true,
                            cwd: "<%= yeoman.app %>/styles-less",
                            src: "main.less",
                            dest: ".tmp/styles",
                            ext: ".css"
                        }
                    ]
                },
                dist: {
                    options: {
                        cleancss: true,
                        report: 'min'
                    },
                    files: [
                        {
                            expand: true,
                            cwd: "<%= yeoman.app %>/styles-less",
                            src: "main.less",
                            dest: ".tmp/styles",
                            ext: ".css"
                        }
                    ]
                }
            },
            useminPrepare: {
                html: "<%= yeoman.app %>/index.html",
                options: {
                    dest: "<%= yeoman.dist %>",
                    flow: {
                        steps: {
                            js: ["concat", "uglifyjs"],
                            css: ["cssmin"]
                        },
                        post: []
                    }
                }
            },
            usemin: {
                html: ["<%= yeoman.dist %>/**/*.html", "!<%= yeoman.dist %>/bower_components/**"],
                css: ["<%= yeoman.dist %>/styles/**/*.css"],
                options: {
                    dirs: ["<%= yeoman.dist %>"]
                }
            },
            htmlmin: {
                dist: {
                    options: {},
                    files: [
                        {
                            expand: true,
                            cwd: "<%= yeoman.app %>",
                            src: ["*.html", "views/*.html", "views/goethe/**/*.html"],
                            dest: "<%= yeoman.dist %>"
                        }
                    ]
                }
            },
            copy: {
                dist: {
                    files: [
                        {
                            expand: true,
                            dot: true,
                            cwd: "<%= yeoman.app %>",
                            dest: "<%= yeoman.dist %>",
                            src: [
                                "favicon.ico",
                                "bower_components/font-awesome/css/*", 
                                "bower_components/font-awesome/fonts/*", 
                                "bower_components/weather-icons/css/*", 
                                "bower_components/weather-icons/font/*", 
                                "bower_components/weather-icons/fonts/*", 
                                "fonts/**/*",
                                "i18n/**/*", 
                                "images/**/*", 
                                "styles/fonts/**/*", 
                                "styles/img/**/*", 
                                "styles/ui/images/*", 
                                "views/**/*"]
                        }, {
                            expand: true,
                            cwd: ".tmp",
                            dest: "<%= yeoman.dist %>",
                            src: ["styles/**", "assets/**"]
                        }, {
                            expand: true,
                            cwd: ".tmp/images",
                            dest: "<%= yeoman.dist %>/images",
                            src: ["generated/*"]
                        }
                    ]
                },
                styles: {
                    expand: true,
                    cwd: "<%= yeoman.app %>/styles",
                    dest: ".tmp/styles/",
                    src: "**/*.css"
                }
            },
            concurrent: {
                server: ["compass:server", "copy:styles"],
                dist: ["compass:dist", "copy:styles", "htmlmin"],
                lessServer: ["less:server", "copy:styles"],
                lessDist: ["less:dist", "copy:styles", "htmlmin"]
            },
            ngconstant: {
                options: {
                    name: 'config',
                    dest: '<%= yeoman.app %>/scripts/config.js',
                    wrap: '"use strict";\n\n{%= __ngModule %}',
                    space: '  ',
                    constants: {
                        api_host: '<%= cnf.api_host %>'
                    },
                    values: {
                        debug: true
                    }
                },
                build: {
                }
            },
            cssmin: {
                options: {
                    keepSpecialComments: '0'
                },
                dist: {}
            },
            concat: {
                options: {
                    separator: grunt.util.linefeed + ';' + grunt.util.linefeed
                },
                dist: {}
            },
            uglify: {
                options: {
                    mangle: true,
                    compress: {
                        drop_console: true
                    }
                },
                dist: {}
            }
        });
        grunt.registerTask("server", function(target) {
            grunt.log.warn('The `server` task has been deprecated. Use `grunt serve` to start a server.');
            if (target === "dist") {
                return grunt.task.run(["serve:dist"]);
            }
            return grunt.task.run(["serve"]);
        });
        grunt.registerTask("serve", function(target) {
            if (target === "dist") {
                return grunt.task.run(["build", "connect:dist:keepalive"]);
            }
            return grunt.task.run(["clean:server", "concurrent:server", "connect:livereload", "watch"]);
        });
        grunt.registerTask("build", ["clean:dist", "ngconstant", "useminPrepare", "concurrent:dist", "copy:dist", "cssmin", "concat", "uglify", "usemin"]);
        return grunt.registerTask("default", ["server"]);
    };

})(); 