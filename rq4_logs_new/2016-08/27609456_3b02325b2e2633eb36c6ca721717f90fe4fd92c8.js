/*!
 * NsWeb Angular Gruntfile
 * @author Jovica Čonkić
 */

'use strict';

/**
 * Livereload and connect variables
 */
var LIVERELOAD_PORT = 35729;
var serveStatic = require('serve-static');
var rewrite = require('connect-modrewrite');
var lrSnippet = require('connect-livereload')({
  port: LIVERELOAD_PORT
});
var mountFolder = function (connect, dir) {
  return serveStatic(require('path').resolve(dir.toString()));
}; 

/**
 * Grunt module
 */
module.exports = function (grunt) {
  var version = grunt.option('semver') || 'patch';

  /**
   * Dynamically load npm tasks
   */
  require('matchdep').filterDev('grunt-*').forEach(grunt.loadNpmTasks);

  /**
   * NsWeb Angular Grunt config
   */
  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),
    /**
     * Set project info
     */
    project: {
      src: 'app/assets',
      app: 'app',
      dist: 'dist',
	    dist_src: 'dist/assets',
      e2e_test: 'e2e-tests',
      unit_test: 'unit-tests',
	    index: [
        '<%= project.app %>/index.html'
      ],
      assets: '<%= project.app %>/assets',
      css: [
        'src/scss/style.scss'
      ],
      js: [
        '<%= project.src %>/js/*.js'
      ]
    },
    
    /**
     * Injector
     * https://github.com/klei/grunt-injector
     * Inject references to files into other files
     * (think scripts and stylesheets into an html file)
     */
    injector: {
      options: {
        ignorePath: ['<%= project.app %>/', '<%= project.dist %>/'],
        addRootSlash: false,
        template: '<%= project.app %>/index.html'
      },
      dev: {
        files: {
          '<%= project.app %>/index.html': [
            '<%= project.assets %>/js/**/*.js',
            '<%= project.assets %>/app/**/*.js',
            '<%= project.assets %>/css/*.css'
          ]
        }
      },
	    dist: {
        options: {
          min: true
        },
        files: {
          '<%= project.dist %>/index.html': [
            '<%= project.dist_src %>/js/**/*.js',
            '<%= project.dist_src %>/app/**/*.js',
            '<%= project.dist_src %>/css/*.css'
          ]
        }
      },
      bower: {
        options: {
          starttag: '<!-- bower:{{ext}} -->',
          endtag: '<!-- endbower -->',
          min: true
        },
        files: {
          '<%= project.app %>/index.html': 'bower.json'
        }
      }
    },

    /**
     * Project banner
     * Dynamically appended to CSS/JS files
     * Inherits text from package.json
     */
    tag: {
      banner: '/*!\n' +
              ' * <%= pkg.name %>\n' +
              ' * <%= pkg.title %>\n' +
              ' * <%= pkg.url %>\n' +
              ' * @author <%= pkg.author %>\n' +
              ' * @version <%= pkg.version %>\n' +
              ' * Copyright <%= pkg.copyright %>. <%= pkg.license %> licensed.\n' +
              ' */\n'
    },
    
    /**
     * Bump
     * https://github.com/vojtajina/grunt-bump
     * Bump package version, create tag, commit, push .
     */
	  bump: {
	    options: {
		    files: ['package.json', 'config.json']
	    }
    },
    
    /**
     * Files revisioning
     * https://github.com/yeoman/grunt-filerev
     * Static asset revisioning through file content hash
     */
	  filerev: {
	    dist: {
        options: {
		      encoding: 'utf8',
          algorithm: 'md5',
		      length: 8
        },
        files: [{
          src: [
		        '<%= project.dist_src %>/app/**/*.js',
			      '<%= project.dist_src %>/js/**/*.js',
			      '<%= project.dist_src %>/css/**/*.css'
          ]
        }]
      }
	  },

    /**
     * Connect port/livereload
     * https://github.com/gruntjs/grunt-contrib-connect
     * Starts a local webserver and injects
     * livereload snippet
     */
    connect: {
      options: {
        port: 9000,
        hostname: '*'
      },
      livereload: {
        options: {
		    base: 'app',
          middleware: function (connect, options) {
            var rules = [
                '!\\.html|\\.js|\\.css|\\.svg|\\.jp(e?)g|\\.png|\\.gif$ /index.html [L]'
            ];
            return [
			        rewrite(rules),
			        lrSnippet,
			        mountFolder(connect, options.base)
			      ];
          }
        }
      },
      test: {
	      options: {
	        base: 'app',
          middleware: function (connect, options) {
            var rules = [
                '!\\.html|\\.js|\\.css|\\.svg|\\.jp(e?)g|\\.png|\\.gif$ /index.html [L]'
            ];
            return [
			        rewrite(rules),
			        mountFolder(connect, options.base)
			      ];
          }
		    }
	    }
    },
    
    /**
     * Copy
     * https://github.com/gruntjs/grunt-contrib-copy
     * Copy files and folders
     */
    copy: {
	    main: {
        expand: true,
		    cwd: '<%= project.app %>/',
        src: ['**/*', '!**/assets/css/**', '!**/assets/js/**', '!**/assets/app/**/*.js'],
		    dest: '<%= project.dist %>',
	    },
	  },
    
    /**
     * NG Constant
     * https://github.com/werk85/grunt-ng-constant
     * Create angular constants
     */
    ngconstant: {
      dev: {
        options: {
          dest: '<%= project.assets %>/js/config.js',
		      wrap: "(function () {\n'use strict';\n {%= __ngModule %} })();",
          name: 'config',
		      space: ' '
        },
		    constants: function () {
		      var pkg = grunt.file.readJSON('config.json');
		      pkg.env = 'DEV';
		      pkg.description = 'We are in development!';
          return {
            envPackage: pkg
          };
        },
        values: {
          debug: false
        }
      },
	    dist: {
        options: {
          dest: '<%= project.assets %>/js/config.js',
		      wrap: "(function () {\n'use strict';\n {%= __ngModule %} })();",
          name: 'config',
		      space: ' '
        },
        constants: function () {
		      var pkg = grunt.file.readJSON('config.json');
		      pkg.env = 'PROD';
		      pkg.description = 'We are in production!';
          return {
            envPackage: pkg
          };
        },
        values: {
          debug: false
        }
      }
    },
	
    /**
     * HTMLmin
     * https://github.com/gruntjs/grunt-contrib-htmlmin
     * Minify HTML
     */
	  htmlmin: {
      dist: {
        options: {
          removeComments: true,
          collapseWhitespace: true
        },
        files: [{
          expand: true,
          cwd: '<%= project.dist %>/',
          src: '*.html',
          dest: '<%= project.dist %>/'
        },
        {
          expand: true,
          cwd: '<%= project.dist_src %>/app/',
          src: '**/*.html',
          dest: '<%= project.dist_src %>/app/'
        }]
      }
    },

    /**
     * JSHint
     * https://github.com/gruntjs/grunt-contrib-jshint
     * Manage the options inside .jshintrc file
     */
    jshint: {
      files: [
        '<%= project.src %>/js/{,*/}*.js',
        '<%= project.src %>/app/{,*/}*.js',
        '<%= project.e2e_test %>/specs/{,*/}*.js',
        '<%= project.unit_test %>/specs/{,*/}*.js'
      ],
      options: {
        jshintrc: '.jshintrc'
      }
    },

    /**
     * Uglify (minify) JavaScript files
     * https://github.com/gruntjs/grunt-contrib-uglify
     * Compresses and minifies all JavaScript files
     */
    uglify: {
      options: {
        banner: "<%= tag.banner %>",
        report: 'min',
        mangle: true,
		    compress: {
		      sequences: true,
		      dead_code: true,
		      conditionals: true,
		      booleans: true,
		      unused: true,
		      if_return: true,
		      join_vars: true,
		      drop_console: true
	      }
      },
      dist: {
        files: [
          {
            expand: true,
            cwd: '<%= project.src %>/js/',
            src: '**/*.js',
            dest: '<%= project.dist_src %>/js/',
            rename: function(destBase, destPath) {
		            return destBase+destPath.replace('.js', '.min.js');
		        }
          },
		      {
            expand: true,
            cwd: '<%= project.src %>/app/',
            src: '**/*.js',
            dest: '<%= project.dist_src %>/app/',
            rename: function(destBase, destPath) {
		            return destBase+destPath.replace('.js', '.min.js');
		        }
          }
        ]
      }
    },
    
    /**
     * Minify PNG and JPEG images
     * https://github.com/gruntjs/grunt-contrib-imagemin
     * Compresses and minify images
     */
    imagemin: {
      dist: {
	      options: {
          optimizationLevel: 3,
          progressive: true,
          interlaced: true
        },
		    files: [{
			    expand: true,
			    cwd: '<%= project.dist_src %>/images/',
			    src: ['**/*.{png,PNG,jpg,JPG,jpeg,JPEG,gif,GIF}'],
			    dest: '<%= project.dist_src %>/images/'
		    },
        {
			    expand: true,
			    cwd: '<%= project.dist %>/',
			    src: ['*.{png,PNG,jpg,JPG,jpeg,JPEG,gif,GIF,ico,ICO}'],
			    dest: '<%= project.dist %>/'
		    }]
	    }
	  },
    
    /**
     * Clean
     * https://github.com/gruntjs/grunt-contrib-clean
     * Clean files and folders
     */
    clean: {
      dist: [
        '<%= project.dist %>'
			]
	  },

    /**
     * Compile Sass/SCSS files
     * https://github.com/gruntjs/grunt-contrib-sass
     * Compiles all Sass/SCSS files and appends project banner
     */
    sass: {
      dev: {
        options: {
          compass: true,
		      noCache: true,
          style: 'expanded'
        },
        files: {
          '<%= project.assets %>/css/style.css': '<%= project.css %>'
        }
      },
      dist: {
        options: {
          compass: true,
		      noCache: true,
		      sourcemap: 'none',
          style: 'compressed'
        },
        files: {
          '<%= project.dist_src %>/css/style.min.css': '<%= project.css %>'
        }
      }
    },

    /**
     * Opens the web server in the browser
     * https://github.com/jsoverson/grunt-open
     */
    open: {
      server: {
        path: 'http://localhost:<%= connect.options.port %>'
      }
    },

    /**
     * Runs tasks against changed watched files
     * https://github.com/gruntjs/grunt-contrib-watch
     * Watching development files and run concat/compile tasks
     * Livereload the browser once complete
     */
    watch: {
      scripts: {
		    files: [
	        '<%= project.assets %>/js/{,*/}*.js',
		      '<%= project.assets %>/app/{,*/}*.js',
          '<%= project.e2e_test %>/specs/{,*/}*.js',
		      '<%= project.unit_test %>/specs/{,*/}*.js'
		    ],
		    tasks: ['jshint'],
		    options: {
		      spawn: false
		    }
      },
      karma: {
        files: [
          '<%= project.src %>/js/{,*/}*.js',
          '<%= project.src %>/app/{,*/}*.js',
		      '<%= project.e2e_test %>/specs/{,*/}*.js',
		      '<%= project.unit_test %>/specs/{,*/}*.js'
        ],
        tasks: ['karma:continuous:run']
      },
      sass: {
        files: 'src/scss/{,*/}*.{scss,sass}',
        tasks: ['sass:dev']
      },
      livereload: {
        options: {
          livereload: LIVERELOAD_PORT
        },
        files: [
          '<%= project.app %>/{,*/}*.html',
          '<%= project.assets %>/app/{,*/}*.html',
          '<%= project.assets %>/css/{,*/}*.css',
          '<%= project.assets %>/js/{,*/}*.js',
          '<%= project.assets %>/app/{,*/}*.js',
          '<%= project.assets %>/images/{,*/}*.{png,PNG,jpg,JPG,jpeg,JPEG,gif,GIF,webp,WEBP,svg,SVG}'
        ]
      }
    },
    
    /**
     * Runs shell commands
     * https://github.com/sindresorhus/grunt-shell  
     * Webdriver-manager update command  
     */
    shell: {
      protractor_update: {
        options: {
          stdout: true
        },
        command: 'node node_modules/protractor/bin/webdriver-manager update'
      }
    },
    
    /**
     * Runs karma unit tests
     * https://github.com/karma-runner/grunt-karma
     */
    karma: {
      options: {
        configFile: '<%= project.unit_test %>/karma.conf.js',
      },
      unit: {
        port: 9000,
        singleRun: true,
        browsers: ['Chrome', 'Firefox']
      },
      continuous: {
        reporters: 'dots',
        singleRun: false,
        browsers: ['PhantomJS'],
        background: true
      }
    },
      
    /**
     * Grunt plugin for running Protractor runner
     * https://github.com/teerapap/grunt-protractor-runner 
     * Run protractor described tests  
     */
    protractor: {
      options: {
        configFile: "node_modules/protractor/example/conf.js",
        noColor: true,
        debug: false,
        args: { }
      },
      e2e: {
        options: {
          configFile: "<%= project.e2e_test %>/protractor.conf.js",
          keepAlive: false
        }
      }
    }
  });

  /**
   * Default task
   * Run `grunt` on the command line
   */
  grunt.registerTask('default', [
    'sass:dev',
    'ngconstant:dev',
    'jshint',
    'injector:dev',
    'injector:bower',
    'connect:livereload',
    'open',
    'karma:continuous:start',
    'watch'
  ]);

  /**
   * Build task
   * Run `grunt build` on the command line
   * Then clean, copy, minify and optimize content for distribution
   */
  grunt.registerTask('build', [
    'clean',
    'bump-only:'+ version,
    'sass:dist',
    'test',
    'copy',
    'jshint',
    'ngconstant:dist',
    'uglify',
    'filerev:dist',
    'injector:dist',
    'injector:bower',
    'imagemin:dist',
    'htmlmin:dist'
  ]);
  
  /**
   * Test task
   * Run `grunt e2e-test` on the command line
   * Then test all described tests using protractor selenium wrapper
   */
  grunt.registerTask('test', [
    'shell:protractor_update',
    'jshint',
	  'connect:test',
	  'protractor:e2e',
    'karma:unit'
  ]);
};