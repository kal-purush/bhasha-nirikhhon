/*jslint indent: 2, maxerr: 50, maxlen: 120, node: true, nomen: true, plusplus: true, vars: true */

'use strict';


// #####################################################################################################################

module.exports = function (grunt) {

  // Load grunt tasks automatically
  require('load-grunt-tasks')(grunt);

  // Time how long tasks take. Can help when optimizing build times
  require('time-grunt')(grunt);

  // Configurable paths for the application
  var appConfig = {
    'app' : require('./bower.json').appPath || 'app',
    'dist': 'dist'
  };

  // Define the configuration for all the tasks
  grunt.initConfig({

    // -------------------------------------------

    // Project settings
    'yeoman'       : appConfig,

    // -------------------------------------------

    // Less compilation
    'less': {
      'all': {
        'files': {
          'app/styles/main.css': 'less/main.less'
        }
      }
    },

    // -------------------------------------------

    // Watches files for changes and runs tasks based on the changed files
    'watch'        : {
      'bower'     : {
        'files': ['bower.json'],
        'tasks': ['wiredep']
      },
      'js'        : {
        'files'  : ['<%= yeoman.app %>/scripts/{,*/}*.js'],
        'tasks'  : ['newer:jshint:all'],
        'options': {
          'livereload': '<%= connect.options.livereload %>'
        }
      },
      'styles'    : {
        'files': ['<%= yeoman.app %>/styles/{,*/}*.css'],
        'tasks': ['newer:copy:styles', 'autoprefixer']
      },
      'less'      : {
        'files': ['less/{,*/}*.less'],
        'tasks': ['less']
      },
      'gruntfile' : {
        'files': ['Gruntfile.js']
      },
      'livereload': {
        'options': {
          'livereload': '<%= connect.options.livereload %>'
        },
        'files'  : [
          '<%= yeoman.app %>/{,*/}*.html',
          '.tmp/styles/{,*/}*.css',
          '<%= yeoman.app %>/images/{,*/}*.{png,jpg,jpeg,gif,webp,svg}'
        ]
      }
    },

    // -------------------------------------------

    // The actual grunt server settings
    'connect': {
      'options'   : {
        'port'      : 9000,
        // Change this to '0.0.0.0' to access the server from outside.
        'hostname'  : 'localhost',
        'livereload': 35729
      },
      'livereload': {
        'options': {
          'open'      : true,
          'middleware': function (connect) {
            return [
              connect.static('.tmp'),
              connect().use(
                '/bower_components',
                connect.static('./bower_components')
              ),
              connect().use(
                '/app/styles',
                connect.static('./app/styles')
              ),
              connect.static(appConfig.app)
            ];
          }
        }
      },
      'dist'      : {
        'options': {
          'open': true,
          'base': '<%= yeoman.dist %>'
        }
      }
    },

    // -------------------------------------------

    // Empties folders to start fresh
    'clean'        : {
      'dist'  : {
        'files': [{
          'dot': true,
          'src': [
            '.tmp',
            '<%= yeoman.dist %>/{,*/}*',
            '!<%= yeoman.dist %>/.git{,*/}*'
          ]
        }]
      },
      'server': '.tmp'
    },

    // -------------------------------------------

    // Add vendor prefixed styles
    'autoprefixer': {
      'options': {
        'browsers': ['last 1 version']
      },
      'server' : {
        'options': {
          'map': true
        },
        'files'  : [{
          'expand': true,
          'cwd'   : '.tmp/styles/',
          'src'   : '{,*/}*.css',
          'dest'  : '.tmp/styles/'
        }]
      },
      'dist'   : {
        'files': [{
          'expand': true,
          'cwd'   : '.tmp/styles/',
          'src'   : '{,*/}*.css',
          'dest'  : '.tmp/styles/'
        }]
      }
    },

    // -------------------------------------------

    // Automatically inject Bower components into the app
    'wiredep'    : {
      'app' : {
        'src'       : ['<%= yeoman.app %>/index.html'],
        'ignorePath': /\.\.\//
      }
    },

    // -------------------------------------------

    // Renames files for browser caching purposes
    'filerev'      : {
      'dist': {
        'src': [
          '<%= yeoman.dist %>/scripts/{,*/}*.js',
          '<%= yeoman.dist %>/styles/{,*/}*.css',
          //'<%= yeoman.dist %>/images/{,*/}*.{png,jpg,jpeg,gif,webp,svg}',
          '<%= yeoman.dist %>/styles/fonts/*'
        ]
      }
    },

    // -------------------------------------------

    // Reads HTML for usemin blocks to enable smart builds that automatically
    // concat, minify and revision files. Creates configurations in memory so
    // additional tasks can operate on them
    'useminPrepare': {
      'html'   : '<%= yeoman.app %>/index.html',
      'options': {
        'dest': '<%= yeoman.dist %>',
        'flow': {
          'html': {
            'steps': {
              'js' : ['concat', 'uglifyjs'],
              'css': ['cssmin']
            },
            'post' : {}
          }
        }
      }
    },

    // -------------------------------------------

    // Performs rewrites based on filerev and the useminPrepare configuration
    'usemin'       : {
      'html'   : ['<%= yeoman.dist %>/{,*/}*.html'],
      'css'    : ['<%= yeoman.dist %>/styles/{,*/}*.css'],
      'options': {
        'assetsDirs': [
          '<%= yeoman.dist %>',
          '<%= yeoman.dist %>/images',
          '<%= yeoman.dist %>/styles'
        ]
      }
    },

    // -------------------------------------------

    'imagemin': {
      'dist': {
        'files': [{
          'expand': true,
          'cwd'   : '<%= yeoman.app %>/images',
          'src'   : '{,*/}*.{png,jpg,jpeg,gif}',
          'dest'  : '<%= yeoman.dist %>/images'
        }]
      }
    },

    // -------------------------------------------

    'svgmin': {
      'dist': {
        'files': [{
          'expand': true,
          'cwd'   : '<%= yeoman.app %>/images',
          'src'   : '{,*/}*.svg',
          'dest'  : '<%= yeoman.dist %>/images'
        }]
      }
    },

    // -------------------------------------------

    'htmlmin'   : {
      'dist': {
        'options': {
          'collapseWhitespace'       : true,
          'conservativeCollapse'     : true,
          'collapseBooleanAttributes': true,
          'removeCommentsFromCDATA'  : true
          //'removeOptionalTags'       : true
        },
        'files'  : [{
          'expand': true,
          'cwd'   : '<%= yeoman.dist %>',
          'src'   : ['*.html', 'views/{,*/}*.html'],
          'dest'  : '<%= yeoman.dist %>'
        }]
      }
    },

    // -------------------------------------------

    // ng-annotate tries to make the code safe for minification automatically
    // by using the Angular long form for dependency injection.
    'ngAnnotate': {
      'dist': {
        'files': [{
          'expand': true,
          'cwd'   : '.tmp/concat/scripts',
          'src'   : '*.js',
          'dest'  : '.tmp/concat/scripts'
        }]
      }
    },

    // -------------------------------------------

    // Copies remaining files to places other tasks can use
    'copy': {
      'dist'  : {
        'files': [{
          'expand': true,
          'dot'   : true,
          'cwd'   : '<%= yeoman.app %>',
          'dest'  : '<%= yeoman.dist %>',
          'src'   : [
            '*.{ico,png,txt}',
            '.htaccess',
            '*.html',
            'views/{,*/}*.html',
            'images/{,*/}*.{webp}',
            'styles/fonts/{,*/}*.*'
          ]
        }, {
          'expand': true,
          'cwd'   : '.tmp/images',
          'dest'  : '<%= yeoman.dist %>/images',
          'src'   : ['generated/*']
        }, {
          'expand': true,
          'cwd'   : 'bower_components/bootstrap/dist',
          'src'   : 'fonts/*',
          'dest'  : '<%= yeoman.dist %>'
        }, {
          'expand': true,
          'cwd'   : 'bower_components/fontawesome',
          'src'   : 'fonts/*',
          'dest'  : '<%= yeoman.dist %>'
        }]
      },
      'styles': {
        'expand': true,
        'cwd'   : '<%= yeoman.app %>/styles',
        'dest'  : '.tmp/styles/',
        'src'   : '{,*/}*.css'
      }
    },

    // -------------------------------------------

    // Run some tasks in parallel to speed up the build process
    'concurrent': {
      'server': [
        'copy:styles'
      ],
      'dist'  : [
        'copy:styles',
        'imagemin',
        'svgmin'
      ]
    }
  });


  // -------------------------------------------------------------------------------------------------------------------

  grunt.registerTask('serve', 'Compile then start a connect web server', function (target) {
    if (target === 'dist') {
      return grunt.task.run(['build', 'connect:dist:keepalive']);
    }

    grunt.task.run([
      'clean:server',
      'wiredep',
      'less',
      'concurrent:server',
      'autoprefixer:server',
      'connect:livereload',
      'watch'
    ]);
  });


  // -------------------------------------------------------------------------------------------------------------------

  grunt.registerTask('build', [
    'clean:dist',
    'wiredep',
    'less',
    'useminPrepare',
    'concurrent:dist',
    'autoprefixer',
    'concat',
    'ngAnnotate',
    'copy:dist',
    'cssmin',
    'uglify',
    'filerev',
    'usemin',
    'htmlmin'
  ]);


  // -------------------------------------------------------------------------------------------------------------------

  grunt.registerTask('default', [
    'build'
  ]);
};