module.exports = function(grunt) {
   require('load-grunt-tasks')(grunt);

   grunt.initConfig({

      // Import package manifest
      pkg: grunt.file.readJSON("fuzzy-js.json"),

      // Banner definitions
      meta: {
         banner: "/*\n" +
            " *  <%= pkg.title || pkg.name %> - v<%= pkg.version %>\n" +
            " *\n" +
            " *  Made by <%= pkg.author.name %>\n" +
            " */\n"
      },

      clean: {
         build: "build"
      },

      // Concat definitions
      concat: {
         dist: {
            src: ["build/_bower.js", "src/*.js"],
            dest: "public/js/fuzzy.js"
         },
         options: {
            banner: "<%= meta.banner %>"
         }
      },

      bower_concat: {
         all: {
           dest: 'build/_bower.js',
           cssDest: 'build/_bower.css',
           bowerOptions: {
             relative: false
           }
         }
      },

      // Lint definitions
      jshint: {
         files: ["src/*.js"],
         options: {
            jshintrc: ".jshintrc"
         }
      },

      // Minify definitions
      uglify: {
         my_target: {
            src: ["public/js/fuzzy.js"],
            dest: "public/js/fuzzy.min.js"
         },
         options: {
            banner: "<%= meta.banner %>"
         }
      },

      // Compile sass
      sass: {
          options: {
              sourceMap: true
          },
          dist: {
              files: {
                  'public/css/main.css': 'src/sass/main.scss'
              }
          }
      },

      copy: {
         build: {
            files: [
               // includes files within path
                    {expand: 'true', cwd: 'src/img/', src: ['*.png', '**/*.png'], dest: 'public/img/'},
                    {expand: 'true', cwd: 'src/', src: [
                        'index.html',
                        'about.html'], dest: 'public/'}
            ]
         }
      },

      // watch for changes to source
      // Better than calling grunt a million times
      // (call 'grunt watch')
      watch: {
          files: ['src/**/*'],
          tasks: ['default'],
          options: {
              livereload: true
          }
      },
      connect: {
          demo: {
              options: {
                  port: 8000,
                  useAvailablePort: true,
                  hostname: "*",
                  //keepalive: true,
                  base: [
                      'public'
                  ],
                  middleware: function(connect, options) {
                      var middlewares = [];
                      middlewares.push(require('connect-livereload')());
                      options.base.forEach(function(base) {
                          middlewares.push(connect.static(base));
                      });
                      return middlewares;
                  }
              }
          }
      },
      open: {
          demo: {
              path: 'http://localhost:<%= connect.demo.options.port%>'
          }
      }

   });

   grunt.loadNpmTasks("grunt-contrib-clean");
   grunt.loadNpmTasks("grunt-contrib-concat");
   grunt.loadNpmTasks("grunt-contrib-jshint");
   grunt.loadNpmTasks("grunt-contrib-uglify");
   grunt.loadNpmTasks("grunt-contrib-copy");
   grunt.loadNpmTasks("grunt-contrib-watch");
   grunt.loadNpmTasks("grunt-contrib-connect");
   grunt.loadNpmTasks("grunt-open");
   grunt.loadNpmTasks("grunt-sass");

   grunt.registerTask("default", ["clean", "jshint", "bower_concat", "sass", "concat", "uglify", "copy"]);
   grunt.registerTask('demo',[ 'connect:demo', 'open:demo', 'watch' ]);
   //grunt.registerTask("travis", ["jshint"]);
};