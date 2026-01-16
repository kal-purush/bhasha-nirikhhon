/**
 * Grunt configuration file for portfolio project.
 */
module.exports = function(grunt) {
  require('jit-grunt')(grunt);

  // Initialize our Grunt configurations
  grunt.initConfig({
    less: {
      development: {
        options: {
          compress: true,
          yuicompress: true,
          optimization: 2
        },
        files: {
          "dist/styles.css": "less/base.less" // Destination file: source file
        }
      }
    },
    babel: {
      options: {
        sourceMap: true
      },
      dist: {
        files: {
          "dist/bundle.js": "js/index.js" // Destination file: source file
        }
      }
    },
    watch: {
      styles: {
        files: ['less/**/*.less'], // Which less directories/files to watch
        tasks: ['less'],
        options: {
          nospawn: true
        }
      },
      scripts: {
        files: ['js/**/*.js'], // Which javascript directories/files to watch
        tasks: ['babel'],
        options: {
          nospawn: true
        }
      }
    }
  });

  grunt.registerTask('default', ['less', 'babel', 'watch']);
};