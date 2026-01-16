/*!
 * Website Gruntfile
 * @author Julio Lara
 */
 
/**
 * Grunt Module
 */
module.exports = function(grunt) {
	
	/**
	 * Configuration
	 */
	grunt.initConfig({
	 
		/**
		 * Get package meta data
		 */
		pkg: grunt.file.readJSON('package.json'),

		/**
		 * Sass
		 */
		sass: {
	    	dev: {
		      	options: {
		        	style: 'expanded'
		      	},
		      	files: {
		        	'src/css/main.css': 'src/sass/main.scss'
		      	}
		    },
		    dist: {
		      	options: {
		        	style: 'compressed'
		      	},
		      	files: {
		        	'src/css/min/main.min.css': 'src/sass/main.scss'
		      	}
		    }
	  	},

		uglify: {
		    js_scripts: {
		      	files: {
		        	'src/js/min/main.min.js': ['src/js/main.js'],
		      	},
		    },
		},

		concat: {
            css: {
                files: {
                    'public/css/main.min.css': [
                        'src/css/min/main.min.css'
                    ],
                },
            },
            js: {
                files: {
                    'public/js/main.min.js': [
                        'src/js/min/main.min.js'
                    ],
                },
            }
        }
		
	});

	grunt.loadNpmTasks('grunt-contrib-sass');
	grunt.loadNpmTasks('grunt-contrib-uglify');
	grunt.loadNpmTasks('grunt-contrib-concat');

	grunt.registerTask('default', ['sass','uglify','concat']);

};