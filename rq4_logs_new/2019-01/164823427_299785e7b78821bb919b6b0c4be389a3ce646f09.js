/**
 * Import all required node modules
 */
const babel = require('gulp-babel')
var babelify = require('babelify')
var browserify = require('browserify')
var gulp = require('gulp')
var concat = require('gulp-concat')
var cssMinify = require('gulp-css')
var sass = require('gulp-sass')
var uglify = require('gulp-uglify')
sass.compiler = require('node-sass')
var buffer = require('vinyl-buffer')
var source = require('vinyl-source-stream')

/**
 * Compile Sass/Scss to Css
 * app/sass/* -> app/css/index.css
 */
gulp.task('compile-sass', () => {
	return gulp.src([
		'app/sass/index.scss',
	])
	.pipe(sass())
	.pipe(gulp.dest('app/css/'))
})

/**
 * Concatenate, minify and copy Css
 * app/css/* -> build/css/style.min.css
 */
gulp.task('copy-css', () => {
	return gulp.src('app/css/*')
	.pipe(concat('style.min.css'))
	.pipe(cssMinify())
	.pipe(gulp.dest('build/css/'))
})

/**
 * Compile and bundle Jsx/Js/ES6 (React entrypoint) to Js
 * app/js/index.js -> build/js/bundle.min.js
 */
gulp.task('compile-jsx', () => {
	var options = {
		entries: "app/js/index.js",
		extensions: [
			'.js',
			'.jsx'
		],
		debug: true
	}

	return browserify(options)
	.transform("babelify", {presets: ["@babel/preset-env", "@babel/preset-react"]})
	.bundle()
	.pipe(source("bundle.min.js"))
	.pipe(buffer())
	.pipe(uglify())
	.pipe(gulp.dest("build/js/"))
})

/**
 * Copy Electron Js-Files
 * app/*.js -> build/*
 */
gulp.task('copy-electron-js', () => {
	return gulp.src('app/*.js')
	.pipe(gulp.dest('build/'))
})

/**
 * Copy Html
 * app/*.html -> build/*.html
 */
gulp.task('copy-html', () => {
	return gulp.src('app/*.html')
	.pipe(gulp.dest('build/'))
})

/**
 * Copy Assets
 * app/assets/** -> build/assets/**
 */
gulp.task('copy-assets', () => {
	return gulp.src('app/assets/**/*')
	.pipe(gulp.dest('build/assets/'))
})

/**
 * Ecex compile tasks
 */
gulp.task('compile', gulp.parallel('compile-sass', 'compile-jsx'))

/**
 * Ecex copy tasks
 */
gulp.task('copy', gulp.parallel('copy-css', 'copy-electron-js', 'copy-html', 'copy-assets'))

/**
 * Compile App to /build
 */
gulp.task('build', gulp.series('compile', 'copy'))

/**
 * Add watch task for Sass/Scss, Jsx and Js Files
 */
gulp.task('watch', done => {
	gulp.watch('app/sass/*.scss', gulp.series('compile-sass'))
	gulp.watch('app/css/*.css', gulp.series('copy-css'))
	gulp.watch('app/js/**/*', gulp.series('compile-jsx'))
	gulp.watch('app/*.html', gulp.series('copy-html'))
	gulp.watch('app/*.js', gulp.series('copy-electron-js'))
	done()
})