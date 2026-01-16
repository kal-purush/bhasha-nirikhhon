var gulp = require("gulp");

gulp.task("css", function(){
  var postcss = require("gulp-postcss");
  var sourcemaps = require("gulp-sourcemaps");

  return gulp.src("src/*.css")
    .pipe( sourcemaps.init() )
    .pipe( postcss([
      require("postcss-import")({
        "plugins": [
          require("stylelint")({
            "config": {
              "extends": "stylelint-config-standard"
            }
          })
        ]
      }),
      require("postcss-utilities"),
      require("postcss-custom-properties"),
      require("postcss-apply"),
      require("postcss-calc"),
      require("postcss-custom-media"),
      require("postcss-media-minmax"),
      require("postcss-nested"),
      require("postcss-color-function"),
      require("postcss-color-gray"),
      require("postcss-initial"),
      require("pixrem"),
      require("postcss-selector-matches"),
      require("postcss-selector-not"),
      require("lost"),

      require("autoprefixer"),
      require("postcss-reporter")({"clearMessages": true}),
      require("cssnano")
    ]) )
    .pipe( sourcemaps.write(".") )
    .pipe( gulp.dest("dist/") )
});