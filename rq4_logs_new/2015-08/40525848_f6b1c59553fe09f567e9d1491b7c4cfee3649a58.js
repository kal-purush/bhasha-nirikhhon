var gulp = require("gulp"),
    babel = require("gulp-babel"),
    concat = require("gulp-concat"),
    uglify = require("gulp-uglify"),
    rename = require("gulp-rename");

gulp.task("transpile-test-file", function() {
  return gulp.src("src/react-mdl.test.jsx")
             .pipe(babel({modules: "system"}))
             .pipe(gulp.dest("dist"));
});

gulp.task("transpile-umd", function() {
  return gulp.src("src/react-mdl.jsx")
             .pipe(babel({modules: "system"}))
             .pipe(uglify())
             .pipe(rename("react-mdl.umd.js"))
             .pipe(gulp.dest("dist"));
});

gulp.task("transpile-system", function() {
  return gulp.src("src/react-mdl.jsx")
             .pipe(babel({modules: "system"}))
             .pipe(uglify())
             .pipe(rename("react-mdl.min.js"))
             .pipe(gulp.dest("dist"));
});

gulp.task("transpile", ["transpile-umd", "transpile-system", "transpile-test-file"]);

gulp.task("watch", function() {
  gulp.watch("src/*.jsx", ["transpile"]);
});

gulp.task("default", ["transpile", "watch"]);