module.exports = function(grunt) {
  grunt.initConfig ({
    pkg:  grunt.file.readJSON('package.json'),

    imagemin: {
      options: {
        banner: '/*! <%= pkg.name %> <%= grunt.template.today("yyy-mm-dd") %> */\n',
        optimizationLevel: 7,
        progressive: true,
      },
      dev: {
        files: [{
          expand: true,
          cwd: 'src/',
          src: ['**/*.{png,jpg,gif}'],
          dest: 'dist/'
        }]
      }
    },
    uglify: {
      options: {
        banner:  '/*! <%= pkg.name %> <%= grunt.template.today("yyy-mm-dd") %> */\n'
      },
      files: {
        expand: true,
        cwd: 'src/',
        src: '**/*.js',
        dest: 'dist/'
      }
    },
    cssmin: {
      options: {
        banner: '/*! <%= pkg.name %> <%= grunt.template.today("dd-mm-yyyy") %> */\n'
      },
      target: {
        files: [{
          expand: true,
          cwd: 'src/',
          src: '**/*.css',
          dest: 'dist/',
          ext: '.min.css'
        }]
      }
    },
    htmlmin: {
      options: {
        banner: '/*! <%= pkg.name %> <%= grunt.template.today("dd-mm-yyyy") %> */\n',
        removeComments: true,
        collapseWhitespace: true
      },
      target: {
        files: [{
          expand: true,
          cwd: 'src/',
          src: '**/*.html',
          dest: 'dist/'
        }]
      }
    }

  });

  grunt.loadNpmTasks('grunt-contrib-imagemin');
  grunt.loadNpmTasks('grunt-contrib-cssmin');
  grunt.loadNpmTasks('grunt-contrib-uglify');
  grunt.loadNpmTasks('grunt-contrib-htmlmin');
  // grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.registerTask('images', ['imagemin']);
  grunt.registerTask('js', ['uglify']);
  grunt.registerTask('css', ['cssmin']);
  grunt.registerTask('htmL', ['htmlmin']);
  grunt.registerTask('default', ['htmlmin', 'cssmin', 'uglify', 'imagemin']);


};