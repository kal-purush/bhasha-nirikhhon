var gulp = require('gulp');
var watch = require('gulp-watch');
var pug = require('gulp-pug');
var sass = require('gulp-sass');
var minifycss = require('gulp-minify-css');
var autoprefixer = require('gulp-autoprefixer');
var imagemin = require('gulp-imagemin');
var pngquant = require('imagemin-pngquant');
var plumber = require('gulp-plumber');
var sourcemaps = require('gulp-sourcemaps');
var rimraf = require('gulp-rimraf');
var rigger = require('gulp-rigger');
var browserSync = require('browser-sync');
var reload = browserSync.reload;

var path = {
	src: {
		html: './src/pugs/pages/**/*.*',
		css: './src/scss/main.scss',
		js: './src/js/main.js',
		img: './src/images/**/*.*',
		video: './src/video/**/*.*',
		font: './src/fonts/**/*.*'
	},
	build: {
		html: './build/',
		css: './build/css/',
		js: './build/js/',
		img: './build/images/',
		video: './build/video/',
		font: './build/fonts/'
	},
	watch: {
		html: './src/pugs/**/*.*',
		css: './src/scss/**/*.scss',
		js: './src/js/**/*.js',
		img: './src/images/**/*.*',
		video: './src/video/**/*.*',
		font: './src/fonts/**/*.*'
	},
	clean: './build'
};

var config = {
	server: {
		baseDir: './build'
	},
	tunnel: true,
	host: 'localhost',
	port: 9000
};

gulp.task('server', function() {
	browserSync(config);
});

gulp.task('clean', function() {
 return gulp.src(path.clean, { read: false }) // much faster 
   .pipe(rimraf());
});

gulp.task('build', ['build:html', 'build:css', 'build:js', 'build:img', 'build:video', 'build:font']);

gulp.task('build:html', function() {
	gulp.src(path.src.html)
	.pipe(plumber())
	.pipe(pug({
		pretty: '\t'
	}))
	.pipe(gulp.dest(path.build.html))
	.pipe(reload({stream: true}))
});

gulp.task('build:css', function() {
	gulp.src(path.src.css)
	.pipe(sourcemaps.init())
	.pipe(sass())
	.pipe(autoprefixer({
		browsers:['last 4 versions'],
		cascade: false
	}))
	.pipe(minifycss())
	.pipe(sourcemaps.write('./'))
	.pipe(gulp.dest(path.build.css))
	.pipe(reload({stream:true}))
});

gulp.task('build:js', function() {
	gulp.src(path.src.js)
	.pipe(rigger())
	.pipe(sourcemaps.init())
	.pipe(sourcemaps.write('./'))
	.pipe(gulp.dest(path.build.js))
	.pipe(reload({stream:true}));
	gulp.start('build:html');
});

gulp.task('build:img', function() {
	gulp.src(path.src.img)
	.pipe(imagemin({
		progressive: true,
		svgoPlugins: [{removeViewBox: false}],
		use: [pngquant()],
		interlaced: true
	}))
	.pipe(gulp.dest(path.build.img))
	.pipe(reload({stream:true}))
});

gulp.task('build:video', function() {
	gulp.src(path.src.video)
	.pipe(gulp.dest(path.build.video))
	.pipe(reload({stream:true}))
});

gulp.task('build:font', function() {
	gulp.src(path.src.font)
	.pipe(gulp.dest(path.build.font))
	.pipe(reload({stream:true}))
});

gulp.task('watch', function() {
	watch([path.watch.html], function(event, cb) {
		gulp.start('build:html');
	});
	watch([path.watch.css], function(event, cb) {
		gulp.start('build:css');
	});
	watch([path.watch.js], function(event, cb) {
		gulp.start('build:js');
	});
	watch([path.watch.img], function(event, cb) {
		gulp.start('build:img');
	});
	watch([path.watch.video], function(event, cb) {
		gulp.start('build:video');
	});
	watch([path.watch.font], function(event, cb) {
		gulp.start('build:font');
	});
});

gulp.task('default', ['build', 'server', 'watch']);