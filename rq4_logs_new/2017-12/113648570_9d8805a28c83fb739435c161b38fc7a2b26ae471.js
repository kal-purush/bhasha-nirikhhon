/**
 * 出现如下错误：
 * Gulp Error: Cannot find module 'jshint/src/cli'
 * 请执行：npm install --save-dev jshint gulp-jshint
 */
var pkg = require('./package');
var jshintConfig = pkg.jshintConfig;

var gulp = require('gulp');
//清理文件
var clean = require('gulp-clean');
//避免因错误而中断 gulp
var plumber = require('gulp-plumber');
//合并文件
var concat = require('gulp-concat');
//改名
var rename = require('gulp-rename');
//JavaScript 代码校验
var jshint = require('gulp-jshint');
//专业压缩 Javascript
var uglify = require('gulp-uglify');
//sourcemaps
var sourcemaps = require('gulp-sourcemaps');
//转换成浏览器可用
var browserify = require('browserify');
var babelify = require('babelify');

var source = require('vinyl-source-stream');
var buffer = require('vinyl-buffer');
var util = require('gulp-util');


var imagemin = require('gulp-imagemin');
var pngquant = require('imagemin-pngquant');
var spritesmith = require('gulp.spritesmith');
var cache = require('gulp-cache');

// Load plugins
var $ = require('gulp-load-plugins')();

gulp.task("clean-leoengine", function () {
  return gulp.src('./public/leoengine/')
    .pipe(clean());
});

gulp.task('leo', ['clean-leoengine'], function () {
  var b = browserify({
    entries: './leoEngine/index.js',
    debug: true,
    transform: [babelify.configure({
      presets: ['es2015']
    })]
  });

  return b.bundle()
    .pipe(source('./leoEngine/index.js'))
    .pipe(buffer())
    .pipe(sourcemaps.init({ loadMaps: true }))
    //.pipe($.jshint(jshintConfig))
    //.pipe($.jshint.reporter('default'))
    //.pipe($.uglify())
    // Add other gulp transformations (eg. uglify) to the pipeline here.
    .on('error', util.log)
    .pipe(concat('leoEngine.js'))
    .pipe(sourcemaps.write('./'))
    .pipe(gulp.dest('./public/leoEngine/'));
});


gulp.task("clean-leoengine2", function () {
  return gulp.src('./dist/leoEngine/')
    .pipe(clean());
});

gulp.task('engine', ['clean-leoengine2'], function () {
  var b = browserify({
    entries: './leoEngine/index.js',
    debug: true,
    transform: [babelify.configure({
      presets: ['es2015']
    })]
  });

  return b.bundle()
    .pipe(source('./leoEngine/index.js'))
    .pipe(buffer())
    .pipe(sourcemaps.init({ loadMaps: true }))
    //.pipe($.jshint(jshintConfig))
    //.pipe($.jshint.reporter('default'))
    //.pipe($.uglify())
    // Add other gulp transformations (eg. uglify) to the pipeline here.
    .on('error', util.log)
    .pipe(concat('leoEngine.js'))
    .pipe(sourcemaps.write('./'))
    .pipe(gulp.dest('./dist/leoEngine/'));
});

// 本实例为完成精灵图的合并
gulp.task('merge-sprite0', function () {
  return gulp.src("public/images/role/body/0/*.png")
    .pipe(spritesmith({
      imgName: 'public/images/role/body/0.png', //合并后大图的名称
      cssName: 'css/sprite.css',
      padding: 0,// 每个图片之间的间距，默认为0px
      cssTemplate: (data) => {
        // data为对象，保存合成前小图和合成打大图的信息包括小图在大图之中的信息
        let arr = [],
          width = data.spritesheet.px.width,
          height = data.spritesheet.px.height,
          url = data.spritesheet.image;
        // console.log(data);
        data.sprites.forEach(function (sprite) {
          arr.push(
            ".icon-" + sprite.name +
            "{" +
            "background: url('" + url + "') " +
            "no-repeat " +
            sprite.px.offset_x + " " + sprite.px.offset_y + ";" +
            "background-size: " + width + " " + height + ";" +
            "width: " + sprite.px.width + ";" +
            "height: " + sprite.px.height + ";" +
            "}\n"
          );
        });
        // return "@fs:108rem;\n"+arr.join("")
        return arr.join("");
      }
    }))
    .pipe(gulp.dest("dest/"));
});

// 图片压缩
gulp.task('img', function () {
  return gulp.src(['dest/public/images/role/body/0.png'])
    .pipe(imagemin({
      optimizationLevel: 3, //类型：Number  默认：3  取值范围：0-7（优化等级）
      progressive: true, //类型：Boolean 默认：false 无损压缩jpg图片
      interlaced: false, //类型：Boolean 默认：false 隔行扫描gif进行渲染
      multipass: false, //类型：Boolean 默认：false 多次优化svg直到完全优化      
      use: [pngquant()] //使用 pngquant 深度压缩 png 图片 (png8)
    }))
    .pipe(gulp.dest('dist/imgmin'));
});
