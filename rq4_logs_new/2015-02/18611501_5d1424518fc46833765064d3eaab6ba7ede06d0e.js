module.exports = function ( grunt ) {

  grunt.initConfig({
    imagemin: {
      static: {
        options: {
          optimizationLevel: 7
        },
        files: [
          {
            expand: true,
            cwd: 'wp-content/uploads/',
            src: ['**/*.{png,jpg,gif}'],
          }
        ]
      }
    }
  });

  grunt.loadNpmTasks('grunt-contrib-imagemin');
  grunt.registerTask('default', ['imagemin']);

};