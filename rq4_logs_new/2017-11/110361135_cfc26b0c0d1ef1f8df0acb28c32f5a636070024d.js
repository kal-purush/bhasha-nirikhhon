let gulp         = require("gulp"),
	browserSync  = require("browser-sync").create(),
	sass         = require("gulp-sass"),
	pug          = require("gulp-pug"),
	sourcemaps   = require("gulp-sourcemaps"),
	cleanCSS     = require("gulp-clean-css"),
	autoprefixer = require("gulp-autoprefixer");
/*------------------------------------------------------------------------*/

gulp.task("serve", ["scss", "pug"], function() {
	browserSync.init({
		server: "./dist" 				// browser-sync html
	});
});

gulp.task("scss", function() {
	return gulp
		.src("./app/scss/*.scss")
		.pipe(sourcemaps.init()) 					// gulp-sourcemaps
		.pipe(sass()) 								// gulp-sass
		.pipe(
			autoprefixer({ 							// gulp-autoprefixer
				browsers: ["last 2 versions"],       
				cascade: false
			})
		)
		.pipe(cleanCSS({ compatibility: "ie8" })) 	// gulp-clean-css
		.pipe(sourcemaps.write())
		.pipe(gulp.dest("./dist/css"))
		.pipe(browserSync.stream()); 				// browser-sync css
});

gulp.task("pug", function buildHTML() {
	return gulp
		.src("./app/*.pug")
		.pipe(pug())								 // pug
		.pipe(gulp.dest("./dist"));
});
 
gulp.task("default", ["serve"]);

/*----------------------------------------------------------------------------*/
gulp.watch("./app/scss/*.scss", ["scss"]);
gulp.watch("./dist/*.html").on("change", browserSync.reload);
gulp.watch("./app/*/*.pug", ["pug"]);