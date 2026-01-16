module.exports = function (grunt) {

    // configure tasks
    grunt.initConfig({
        pkg: grunt.file.readJSON("package.json"),
        less: {
            prod: {
                files: {
                    'assets/css/main.css': 'assets/css/main.less'
                }
            }
        },
        cssmin: {
            options: {
                sequence: false,
                level: {
                    1: {
                        specialComments: 0
                    }
                }
            },
            prod: {
                files: {
                    'evl/assets/css/main.min.css': 'assets/css/main.css'
                }
            }
        },
        copy: {
            prod: {
                files: [{
                        expand: true,
                        src: ['videolink.php'],
                        dest: 'evl/',
                        filter: 'isFile'
                    },
                    {
                        expand: true,
                        cwd: 'assets/',
                        src: ['fonts/**'],
                        dest: 'evl/assets/',
                        filter: 'isFile'
                    }
                ],
            },
        },
        compress: {
            prod: {
                options: {
                    archive: 'releases/evl_' + grunt.template.today("yyyymmdd_HHMMss") + '.zip',
                    mode: 'zip'
                },
                files: [{
                    src: 'evl/**'
                }]
            }
        },
        watch: {
            styles: {
                files: ['assets/css/*.less'], // which files to watch
                tasks: ['less', 'cssmin', 'copy'],
                options: {
                    nospawn: true
                }
            }
        }
    });

    // load plugins
    grunt.loadNpmTasks('grunt-contrib-less');
    grunt.loadNpmTasks('grunt-contrib-cssmin');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-contrib-copy');
    grunt.loadNpmTasks('grunt-contrib-compress');

    // register tasks
    grunt.registerTask('build', ['less', 'cssmin', 'copy']);
    grunt.registerTask('default', ['prod']);
    grunt.registerTask('prod', ['build', 'compress']);
}