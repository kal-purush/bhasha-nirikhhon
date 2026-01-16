/**
 * Created by 1t8l7j2 on 2017/7/5.
 */
module.exports = function (grunt) {
    // 加载插件
    [
        'grunt-cafe-mocha',
        'grunt-contrib-jshint'
    ].forEach(function (task) {
        grunt.loadNpmTasks(task);
    });

    // 配置插件
    grunt.initConfig({
        cafemocha:{
            all:{src:'qa/tests-*.js',options:{ui:'tdd'}}
        },
        jshint:{
            app:['index.js','public/js/**/*.js','lib/**/*.js'],
            qa:['Gruntfile.js','public/qa/**/*.js','qa/**/*.js']
        }
    });

    // 注册任务
    grunt.registerTask('default',['cafemocha','jshint']);
};