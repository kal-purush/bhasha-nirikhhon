var gulp = require("gulp");
var stylus = require("gulp-stylus");
var autoprefixer = require("gulp-autoprefixer");
var watch = require("gulp-watch");
var plumber = require("gulp-plumber");

gulp.task("build", function() {
	gulp.src("src/index.styl")
		.pipe(stylus())
		.pipe(autoprefixer({
			browsers: ["last 2 versions"],
			cascade: false
		}))
		.pipe(gulp.dest("dist"));
});

gulp.task("watch", function() {
	gulp.src("src/index.styl")
		.pipe(watch('src/**/*.styl'))
		.pipe(plumber())
		.pipe(stylus())
		.pipe(gulp.dest("dist"));
});