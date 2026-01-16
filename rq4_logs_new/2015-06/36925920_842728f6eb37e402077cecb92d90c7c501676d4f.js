module.exports = function (grunt) {

	grunt.loadNpmTasks('grunt-contrib-sass');
	grunt.loadNpmTasks('grunt-contrib-watch');
	

	grunt.initConfig({
		pkg: grunt.file.readJSON('package.json'),

		sass:{
			dist:{
				options:{
					style: 'expanded'
				},
				files:{
					'app/css/style.css' : 'scss/style.scss',
					

				}
			}
		},
		watch: {
		  css: {
		    	files: ['scss/*.scss'],
		    	tasks: ['sass'],
		    	options: {
		      		livereload: false
		    	}
		  	}
		}
	



	});

	grunt.registerTask('default', ['sass']);
	// grunt.registerTask('docs', ['ngdocs:map', 'ngdocs:places']);

}	