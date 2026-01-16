module.exports = function(grunt) {
  
  grunt.config.set('sass', {
    options: {
      sourceMap: true,
      includePaths: require('node-neat').includePaths
    },
    dev: {
      files: [{
        expand: true,
        cwd: 'assets/styles/',
        src: ['importer.scss'],
        dest: '.tmp/public/styles/',
        ext: '.css'
      }]
    }
  });

  grunt.loadNpmTasks('grunt-sass');
};