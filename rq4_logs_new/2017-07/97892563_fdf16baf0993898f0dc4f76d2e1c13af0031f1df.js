module.exports = function(grunt) {

  // Project configuration.
  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),
    serve: {
      options: {
        port: 9000
      }
    },
    less: {
      min: {
        options: {
          compress:true
        },
        files: {
          'assets/css/style.css': 'assets/less/import.less'
        }
      },
      dev: {
        options: {
          dumpLineNumbers:"comments"
        },
        files: {
          'assets/css/style.dev.css': 'assets/less/import.less'
        }
      }
    },
    watch: {
      scripts: {
        files: ['assets/less/*.less'],
        tasks: ['less'],
        options: {
          spawn: false,
        },
      },
    },
    includes: {
      dist:{
        cwd:'src/views',
        src:['**/*.html'],
        dest:'dist/views',
        options:{
        flatten:true,
        includePath:'src/views/includes/'
      }
    }
  },
  });

  // Load the plugin that provides the "uglify" task.
  grunt.loadNpmTasks('grunt-serve');
  grunt.loadNpmTasks('grunt-contrib-less');
  grunt.loadNpmTasks('grunt-contrib-watch');
  // Default task(s).
  grunt.registerTask('default', ['serve']);
  grunt.loadNpmTasks('grunt-includes');

};