module.exports = function(grunt) {

  // Project configuration
  grunt.initConfig({

    // SCSS compilation
    // Let grunt handle this so we can get a .css file and copy it into the _includes dir.
    // This allows us to include the .css file inline when jekyll builds out
    sass: {
      dist: {
        options: {
          style: 'compressed'
        },
        files: {
          '_includes/main.css': 'css/main.scss'
        }
      }
    }

  });

  // Load the plugin that provides the "sass" task.
  grunt.loadNpmTasks('grunt-contrib-sass');

  // Registered grunt tasks
  grunt.registerTask('default', ['sass']);
};