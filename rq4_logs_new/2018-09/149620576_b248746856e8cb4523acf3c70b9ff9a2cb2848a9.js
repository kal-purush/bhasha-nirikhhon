module.exports = function(grunt) {

  // 01 Project configuration.
  grunt.initConfig({

    pkg: grunt.file.readJSON('package.json'),    

    //Define Path
    dirs: {
      input             : 'development',
      inputAdminSCSS    : 'development/sass/admin',
	  inputMemberSCSS   : 'development/sass/member',
      output            : 'public',
      outputAdminCSS    : 'public/themes/electron/assets/css',
      outputMemberCSS   : 'public/themes/smember/assets/css',
    },

    // Plugin 01: CSSmin
    cssmin: {
      target: {
        files: [
        {
          expand: true,
          cwd: '<%= dirs.outputMemberCSS %>',
          src: ['*.css', '!*.min.css'],
          dest: '<%= dirs.outputMemberCSS %>',
          ext: '.min.css'
        }]
      }
    },

    // Plugin 02: Uglify
    uglify: {
        options: {
          beautify: false,
          compress: {
            drop_console: false
          }
        },
        my_target: {
          files: {
            
          }
        }
      },

    // Plugin 04: Sass
    sass: {
        options: {
          /*sourceMap: true,
          sourceMapEmbed: true,*/
          outputStyle: 'expanded'
        },
        dist: {
          files: {
            '<%= dirs.outputAdminCSS %>/style-new.css' :'<%= dirs.inputAdminSCSS %>/style-new.scss',
          }
        }
      },

    // Plugin 05: watch
    watch: {
        scripts: {
          files: [
            '<%= dirs.inputAdminSCSS %>/*.scss',       // development/sass/admin/*.scss
            '<%= dirs.inputAdminSCSS %>/*/*.scss',     // development/sass/admin/*/*.scss
            '<%= dirs.inputMemberSCSS %>/*.scss',      // 'development/sass/member/*.scss'
            '<%= dirs.inputMemberSCSS %>/*/*.scss',    // 'development/sass/member/*/*.scss',
          ],
          tasks: ['sass', 'includes', 'uglify' 
          //, 'cssmin'
          ],
          options: {
            spawn: false,
            livereload: true
          },
        },
      },

    // Plugin 06: connect
    // connect: {
    //     server: {
    //       options: {
    //         hostname: 'localhost',
    //         port: 3069,
    //         base: '<%= dirs.output %>/',
    //         livereload: true
    //       }
    //     }
    //   },

    // Plugin 07: includes
    includes: {
        files: {
          src: [
            '<%= dirs.input %>/index.html',
          ], // Source files
          dest: '<%= dirs.output %>/',
          flatten: true,
          cwd: '.',
          options: {
              silent: true,
              banner: ''
          }
        }
      },

  });

  //02 Load the plugin
  grunt.loadNpmTasks('grunt-contrib-cssmin');
  grunt.loadNpmTasks('grunt-contrib-uglify');
  grunt.loadNpmTasks('grunt-sass');
  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-contrib-connect');
  grunt.loadNpmTasks('grunt-includes');
  //grunt.loadNpmTasks('grunt-contrib-htmlmin');

  // 03 Register task(s).
  grunt.registerTask('default', 'Log some stuff.', function() {
    grunt.log.write('Logging some stuff...').ok();
  });

  // Task Developer
  grunt.registerTask('dev', [
    'includes',
    'sass',
    'uglify',
    // 'connect',
    'watch',
  ]);

  // Task Publish Project
  grunt.registerTask('publish', [
    'cssmin',
    'uglify',
    //'htmlmin'
  ]);

};