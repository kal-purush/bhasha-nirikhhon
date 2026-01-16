'use strict;'

module.exports = function(grunt) {
  grunt.loadNpmTasks("grunt-contrib-jshint");
  grunt.loadNpmTasks("grunt-jscs");

  var srcFiles = ["app/**/*.js", "lib/**/*.js", "server.js"];

  grunt.initConfig({
    jshint: {
      files: srcFiles,
      options: {
        globalstrict: true,
        quotmark: false,
        smarttabs: true,
        trailing: true,
        undef: true,
        unused: true,
        indent: 2,
        node: true
      }
    },
    jscs: {
      src: srcFiles,
      options: {
        preset: "airbnb",
        requireDotNotation: null,
        disallowMultipleVarDecl: null,
        requireMultipleVarDecl: null
      }
    }
  });

  grunt.registerTask("default", [ "jshint", "jscs"]);
};