module.exports = function( grunt ) {

	grunt.initConfig({
	  concat: {
	    js: {
	      src: ['public/js/app.js', 'public/js/app.routes.js', 'public/js/main.js', 'foundation.min.js', ],
	      dest: 'build/js/scripts.js',
	    }
	    ,css: {
	      src: ['public/css/animate.css', 'public/css/foundation.css', 'public/css/foundation.min.css', 'main.css', 'normalize.css'],
	      dest: 'build/css/styles.css',
	    }
	  },
	  uglify: {
	    js: {
	    	src: ['build/js/scripts.js']
	    	,dest: 'build/js/scripts.js'
	    }
	  },
	  cssmin: {
		  target: {
		    files: [{
		      expand: true,
		      cwd: 'build/css/',
		      src: ['*.css', '!*.min.css'],
		      dest: 'build/css/',
		      ext: '.min.css'
		    }]
		  }
		},
	  watch: {
		  js: {
		    files: ['public/js/*.js'],
		    tasks: ['concat:js', 'uglify'],
		  }
		  ,css: {
		  	files: ['public/css/*.css'],
		  	tasks: ['concat:css', 'cssmin']
		  }
		},
	});

	grunt.loadNpmTasks('grunt-contrib-concat');
	grunt.loadNpmTasks('grunt-contrib-uglify');
	grunt.loadNpmTasks('grunt-contrib-cssmin');
	grunt.loadNpmTasks('grunt-contrib-watch');
	grunt.registerTask('default', ['concat', 'uglify', 'cssmin', 'watch']);

}