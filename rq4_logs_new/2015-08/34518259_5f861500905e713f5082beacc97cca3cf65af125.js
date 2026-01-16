module.exports = function(grunt) {
  grunt.initConfig({
    gitpull: {
      your_target: {
        options: {
        }
      }
    },
  })

  bowerInstall: {
    target: {
    }
  }

  // Do grunt-related things in here
  grunt.loadNpmTasks('grunt-bower-install');
  grunt.loadNpmTasks('grunt-git');

  grunt.registerTask('default', ['gitpull']);
  grunt.registerTask('default', ['bowerInstall']);
};
 