var
LIVERELOAD_PORT = 35729,
    lrSnippet = require('connect-livereload')({
        port: LIVERELOAD_PORT
    }),
    mountFolder = function(connect, dir) {
        return connect.static(require('path').resolve(dir));
    };

module.exports = function(grunt) {
    require('load-grunt-tasks')(grunt);
    grunt.initConfig({
        connect: {
            options: {
                port: 5001,
                hostname: '127.0.0.1',
                open: false
            },
            livereload: {
                options: {
                    middleware: function(connect) {
                        return [
                            lrSnippet,
                            mountFolder(connect, 'dist')
                        ];
                    }
                }
            }

        },
        watch: {
            files: ['src/**/*.*'],
            tasks: ['copy:main'],
            options: {
                livereload: LIVERELOAD_PORT
            }
        },
        copy: {
            main: {
                files: [{
                    expand: true,
                    cwd: 'src/',
                    src: ['**/*.*'],
                    dest: 'dist/'
                }]
            }
        }
    });

    grunt.registerTask('default', ['copy','connect', 'watch']);
    
}