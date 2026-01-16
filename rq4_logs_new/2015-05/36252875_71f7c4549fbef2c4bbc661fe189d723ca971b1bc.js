module.exports = function (grunt) {
    grunt.initConfig({
        jshint: {
            files: ['GruntFile.js', 'app/*.js', 'app/**/*.js', 'app/**/**/*.js',
                    'tests/**/*.js', "!**/libs/*.js", "!**/**/libs/*.js"],
            options: {
                curly: true,
                eqeqeq: true,
                browser: true,
                globals: {
                    afterEach: true,
                    after: true,
                    beforeEach: true,
                    before: true,
                    describe: true,
                    it: true,
                    expect: true,
                    console: true
                }
            }
        },
    });
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.registerTask('default', ['jshint']);
};