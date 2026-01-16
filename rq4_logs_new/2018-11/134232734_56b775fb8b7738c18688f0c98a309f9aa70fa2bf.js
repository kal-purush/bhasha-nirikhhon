//gulpfile.js
const gulp = require('gulp')
const path = require('path')
const shell = require('gulp-shell')
const del = require('del')
const rename = require('gulp-rename')

//目录常量
const SERVER_PATHS = {
    files: ['./server/**/*.js'],
    watchFiles: ['./server/**/*.js'],
    output: './build/server'
}

const STATIC_RES = {
    files: [path.resolve(__dirname, 'res', '**/*')],
    output: path.resolve(__dirname, 'build', 'www')
}

const PACKAGE_JSON = {
    files: [path.resolve(__dirname, 'package.json')],
    output: path.resolve(__dirname, 'build')
}

gulp.task('copy-all', [
    'copy-server',
    'copy-resource',
    'copy-template',
    'copy-packageJson'
])

gulp.task('clean-build', function(cb) {
    del([__dirname + '/build/**/*'], cb)
})

//监视服务器文件变化
gulp.task('watch-server', ['copy-server'], function() {
    gulp.watch(SERVER_PATHS.watchFiles, ['copy-server'])
})

//拷贝服务器文件
gulp.task('copy-server', function() {
    return gulp.src(SERVER_PATHS.files).pipe(gulp.dest(SERVER_PATHS.output))
})

//将res拷贝到build/www中
gulp.task('copy-resource', function() {
    gulp.src(STATIC_RES.files).pipe(gulp.dest(STATIC_RES.output))
})

//将res和index.html拷贝到build/www中
gulp.task('copy-template', function() {
    gulp.src(__dirname + '/index.html')
        .pipe(rename('__index__template__.html'))
        .pipe(gulp.dest(__dirname + '/build/www'))
})

//将res和index.html拷贝到build/www中
gulp.task('copy-packageJson', function() {
    gulp.src(PACKAGE_JSON.files).pipe(gulp.dest(PACKAGE_JSON.output))
})

gulp.task(
    'git:diff',
    shell.task('git diff --name-status --cached HEAD > script/git_diff.log')
)