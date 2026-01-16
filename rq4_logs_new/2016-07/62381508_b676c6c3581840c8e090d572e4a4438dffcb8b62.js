module.exports = function(grunt) {
    grunt.initConfig({
        
        pkg: grunt.file.readJSON('package.json'),
        secret: grunt.file.readJSON('secret.json'),
        remoteTomcatDir: '/opt/tomcat/webapps/',
        remoteTomcatWebappDir: '<%= remoteTomcatDir %>study-highlights-ui/',
        localProjectWebappDir: '../src/main/webapp/',
        localTempDir: '../target/grunt-tmp',
        files: {
            jsAppControllers: '<%= localProjectWebappDir %>/ui/**/*.js',
            jsAppLibraries: '<%= localProjectWebappDir %>/libraries/*.js',
            jsAppMain: '<%= localProjectWebappDir %>/ui/app.js',
            htmlAppViews: '<%= localProjectWebappDir %>/ui/views/*.html',
            htmlAppMain: '<%= localProjectWebappDir %>/ui/index.html',
            scssAppMain: '<%= localProjectWebappDir %>/ui/study-highlights-ui.scss',
            cssAppMain: '<%= localTempDir %>/study-highlights-ui.css',
            war: "../target/study-highlights-ui-*-SNAPSHOT.war",
            warTomcat: "../target/study-highlights-ui.war",
            remoteTomcatWebappWar: '<%= remoteTomcatDir %>study-highlights-ui.war',
        },

        watch: {
            jsAppControllers: {
                files: ['<%= files.jsAppControllers %>'],
                tasks: ['sftp:jsAppControllers']
            },
            jsAppLibraries: {
                files: ['<%= files.jsAppLibraries %>'],
                tasks: ['sftp:jsAppLibraries']
            },
            jsAppMain: {
                files: ['<%= files.jsAppMain %>'],
                tasks: ['sftp:jsAppMain']
            },
            htmlAppViews: {
                files: ['<%= files.htmlAppViews %>'],
                tasks: ['sftp:htmlAppViews']
            },
            htmlAppMain: {
                files: ['<%= files.htmlAppMain %>'],
                tasks: ['sftp:htmlAppMain']
            },
            scssAppMain: {
                files: ['<%= files.scssAppMain %>'],
                tasks: ['sass:scssAppMain', 'sftp:scssAppMain']
            }
        },
        
        sass: {
            scssAppMain: {
                files: {
                    '<%= files.cssAppMain %>': '<%= files.scssAppMain %>'
                }
            }
        },

        rename: {
            war: {
                files: [
                    {
                        src: ['<%= files.war %>'],
                        dest: ['<%= files.warTomcat %>']
                    }
                ]
            }
        },

        sftp: {
            jsAppControllers: {
                files: {
                    "./": "<%= files.jsAppControllers %>",
                }
            },

            jsAppLibraries: {
                files: {
                    "./": "<%= files.jsAppLibraries %>",
                }
            },

            jsAppMain: {
                files: {
                    "./": "<%= files.jsAppMain %>",
                }
            },

            htmlAppViews: {
                files: {
                    "./": "<%= files.htmlAppViews %>",
                }
            },

            htmlAppMain: {
                files: {
                    "./": "<%= files.htmlAppMain %>",
                }
            },

            cssAppMain: {
                files: {
                    "./": "<%= files.htmlAppMain %>",
                }
            },

            scssAppMain: {
                files: {
                    "./": "<%= files.cssAppMain %>"
                },
                options: {
                    path: '<%= remoteTomcatWebappDir %>ui/',
                    srcBasePath: '../target/grunt-tmp/'
                }
            },

            war: {
                files: {
                    "./": "<%= files.warTomcat %>"
                },
                options: {
                    path: '<%= remoteTomcatDir %>',
                    srcBasePath: '../target',
                    showProgress: true
                }
            },

            options: {
                path: '<%= remoteTomcatWebappDir %>',
                srcBasePath: '<%= localProjectWebappDir %>',
                host: '<%= secret.host %>',
                port: '<%= secret.port %>',
                username: '<%= secret.root.username %>',
                password: '<%= secret.root.password %>'
            },
        },
        
        exec: {
            createWarWithoutTests: {
                cmd: "﻿mvn clean install -DskipTests -Dcobertura.skip -Dcheckstyle.skip﻿ -Dfindbugs.skip",
                cwd: "../"
            },
            createWar: {
                cmd: "﻿mvn clean install",
                cwd: "../"
            }
        },

        sshexec: {
            tomcatKill: {
                command: ['kill -9 \`ps ax|grep Dcatalina.home=/opt/tomcat|grep -v grep|cut -b 1-5\`'],
                options: {
                    host: '<%= secret.host %>',
                    port: '<%= secret.port %>',
                    username: '<%= secret.root.username %>',
                    password: '<%= secret.root.password %>'
                }
            },

            removeWar: {
                command: ['rm <%= files.remoteTomcatWebappWar %>; rm -rf <%= remoteTomcatWebappDir %>'],
                options: {
                    host: '<%= secret.host %>',
                    port: '<%= secret.port %>',
                    username: '<%= secret.root.username %>',
                    password: '<%= secret.root.password %>'
                }
            },

            tomcatStart: {
                command: ['/bin/su - tomcat -c "/opt/tomcat/bin/startup.sh"'],
                options: {
                    host: '<%= secret.host %>',
                    port: '<%= secret.port %>',
                    username: '<%= secret.root.username %>',
                    password: '<%= secret.root.password %>'
                }
            }
        }
    });

    grunt.task.registerTask('installWar', ['exec:createWarWithoutTests', 'deployWar']);
    grunt.task.registerTask('deployWar', ['sshexec:removeWar', 'rename:war', 'sftp:war']);
    grunt.task.registerTask('default', ['watch']);

    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-contrib-rename');
    grunt.loadNpmTasks('grunt-sass');
    grunt.loadNpmTasks('grunt-ssh');
    grunt.loadNpmTasks('grunt-exec');
};