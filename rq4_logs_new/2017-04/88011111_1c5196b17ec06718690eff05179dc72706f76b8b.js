module.exports = function(grunt) {

  grunt.initConfig({
    jshint: {
      files: ['Gruntfile.js', 'html/*.js', 'test/*.js','*.js'],
      options: {
	  
	    reporter: require('jshint-html-reporter'),
        reporterOutput: 'jshint-report.html',
		
        globals: {
          jQuery: true
        }
      }
    },
    watch: {
      files: ['<%= jshint.files %>'],
      tasks: ['jshint']
    },
	
	zip: {
      './code.zip': ['./**/*']
    }
	
  });

  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-zip');

  grunt.registerTask('default', ['jshint']);

};