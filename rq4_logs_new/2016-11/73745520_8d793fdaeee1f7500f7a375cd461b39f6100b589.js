var gulp = require("gulp");
var sass = require("gulp-sass");
var eyeglass = require("eyeglass");


var sassOptions = {
  // put node-sass options you need here.

  eyeglass: {
    // put eyeglass options you need here.
  }
};


gulp.task("eyeglass", function () {
  gulp.src("./scss/**/*.scss")
    .pipe(sass(eyeglass(sassOptions)).on("error", sass.logError))
    .pipe(gulp.dest("./css-eyeglass"));
});


gulp.task("sass", function() {
  gulp.src("./scss/**/*.scss")
      .pipe(sass(sassOptions).on("error", sass.logError))
      .pipe(gulp.dest("./css"));
});