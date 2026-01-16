/*
	Michael Hemingway (c) 2017
	==
	run the whole shabang with `$ gulp watch` in the project directory
	install them all with
	$ npm i -D gulp gulp-sass gulp-concat gulp-rename gulp-uglify browser-sync
*/

const gulp = require('gulp')
const sass = require('gulp-sass')
const concat = require('gulp-concat')
const rename = require('gulp-rename')
const uglify = require('gulp-uglify')
const browserSync = require('browser-sync').create()

// environment variables
const project = 'public'  // replace with your localhost folder
const path = ''        // relative path to be in the required project
const assets = '/assets' // img, css, js, pdf ...
const minijs = true       // minify javascript
const minicss = true     // minify css
const scss = true         // compile style.css from scss folder in build tools
const js = true           // compile common.js from folder in build tools


// compiles scss files to css
gulp.task('sass', function () {
	if (scss) {
		return gulp.src('scss/**/*.scss')
		.pipe(
			sass({ outputStyle: (minicss) ? 'compressed' : 'compact'}).on('error', function (err) {
				console.error(err.message)
				// Display error in the browser
				browserSync.notify(err.message, 3000)
				// Prevent gulp from catching the error and exiting the watch process
				this.emit('end')
			}))
		.pipe(gulp.dest( path + project + assets + '/css/'))
		.pipe(browserSync.reload({ stream: true }))
	}
})

// compiles and concatenates js files and reloads the browser
gulp.task('scripts', function () {
	let scr = (js) ? 'js/**/*.js' : path + project + assets + '/js/**/*.js'
	let data = gulp.src(scr)

	if (js) {
		data.pipe(concat('common.js'))
		if (minijs) {
			data
				.pipe(rename('common.min.js'))
				.pipe(uglify().on('error', function (err) {
					console.error(err.message)
					// Display error in the browser
					browserSync.notify(err.message, 3000)
					// Prevent gulp from catching the error and exiting the watch process
					this.emit('end')
				}))
		}
		data.pipe(gulp.dest(path + project + assets + '/js/'))
	}
	data.pipe(browserSync.reload({ stream: true }))
	return data
})

// does browser live-reloading
gulp.task('browserSync', () => 
	browserSync.init({ server: { baseDir:"./public" } })
)

// watches files for changes, adjust accordingly
gulp.task('watch', ['browserSync', 'sass', 'scripts'], function () {
	gulp.watch('scss/**/*.scss', ['sass'])
	gulp.watch('js/**/*.js', ['scripts'])
	// gulp.watch(path + project + assets + '/js/**/*.js', ['scripts'])
	gulp.watch(path + project + '**/*.html').on('change', browserSync.reload)  // pages
})

// stop old version of gulp watch from running when you modify the gulpfile
gulp.watch("gulpfile.js").on( "change", () => process.exit(0) )