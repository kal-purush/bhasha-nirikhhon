module.exports = function (grunt){

	//配置路径
	var config = {
		src: 'src/',
		dist: 'dist/'
	};

	//加载技能
	require('load-grunt-tasks')(grunt);

	//分配任务
	grunt.initConfig({
		copy: {
			js: {
				// files: [
				// 	{
				// 		expand: true,
				// 		cwd: config.src,
				// 		src: '**/*.js',
				// 		dest: config.dist,
				// 		ext: '.min.js'
				// 	}

				// ],

				expand: true,
				cwd: config.src,
				src: '**/*.js',
				dest: config.dist,
				ext: '.min.js'
			}
		},

		clean: {
			js: {
				src: config.dist + '**/*.js'
			}
		}

	});

	//开启任务
	grunt.registerTask('default', 'copy');
	grunt.registerTask('cleanJs', 'clean');

};