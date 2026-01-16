module.exports = (grunt) => {
    "use strict";

    grunt.initConfig({
        // pkg: grunt.file.readJson('package.json'),
        ts: {
            default: {
                tsconfig: './tsconfig.json',
                watch: './src'
            }
        }
    });
    grunt.loadNpmTasks("grunt-ts");
    grunt.registerTask("default", ["ts"]);
}