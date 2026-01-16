module.exports = function (grunt) {
  grunt.registerTask('lint', [
    'newer:coffee',
    'newer:eslint',
    'newer:sass',
    'newer:csslint'
  ]);
};