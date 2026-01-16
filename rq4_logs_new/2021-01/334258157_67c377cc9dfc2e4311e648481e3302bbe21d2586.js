const gulp = require("gulp");
const plumber = require("gulp-plumber");
const sourcemap = require("gulp-sourcemaps");
const less = require("gulp-less");
const postcss = require("gulp-postcss");
const autoprefixer = require("autoprefixer");
const sync = require("browser-sync").create();
const csso = require("gulp-csso");
const rename = require("gulp-rename");
const imagemin = require("gulp-imagemin");
const webp = require("gulp-webp");
const del = require("del");
const svgstore = require("gulp-svgstore")


// delete

const clean = () => {
  return del("build");
};

exports.clean = clean;


// Copy

const copy = () => {
  return gulp.src([
    "source/fonts/**/*",
    "source/img/*",
    "source/js/*.js",
    "source/*.ico",
    "source/*.html",
    "source/css/*"

  ], {
    base: "source"
  })
  .pipe(gulp.dest("build"));
};

exports.copy = copy;

// Styles

const styles = () => {
  return gulp.src("source/less/style.less")
    .pipe(plumber())
    .pipe(sourcemap.init())
    .pipe(less())
    .pipe(postcss([
      autoprefixer()
    ]))
//    .pipe(csso())
//    .pipe(rename("styles.min.css"))
    .pipe(sourcemap.write("."))
    .pipe(gulp.dest("source/css"))
    .pipe(sync.stream());
}

exports.styles = styles;

// sprite

const sprite = () => {
  return gulp.src("source/img/**/icon-*.svg")
  .pipe(svgstore())
  .pipe(rename("sprite.svg"))
  .pipe(gulp.dest("build/img"))
}

exports.sprite = sprite;

// Images

const images = () => {
  return gulp.src("source/img/**/*.{jpg,png,svg}")
  .pipe(imagemin([
    imagemin.optipng({optimizationLevel: 3}),
    imagemin.mozjpeg({progressive: true}),
    imagemin.svgo()
  ]))
  .pipe(gulp.dest("build/img"))
}

exports.images = images;


// WebP

const webpicture = () => {
  return gulp.src("source/img/**/*.{jpg,png}")
  .pipe(webp({quality: 90}))
  .pipe(gulp.dest("build/img"))
};

exports.webp = webpicture;












// Server

const server = (done) => {
  sync.init({
    server: {
      baseDir: 'source'
    },
    cors: true,
    notify: false,
    ui: false,
  });
  done();
}

exports.server = server;

// Watcher

const watcher = () => {
  gulp.watch("source/less/**/*.less", gulp.series("styles"));
  gulp.watch("source/*.html").on("change", sync.reload);
}

exports.default = gulp.series(
  styles, server, watcher
);

//build



exports.build = gulp.series (
 clean, copy, styles, sprite
 // "html"
);

//start


exports.start = gulp.series(
  clean, copy, styles, sprite, server, watcher
);


//
//


//
//
//
//
//
//






//
//
//
//
//
//
//
//
//
//
//
//
//
//
// const gulp = require("gulp");
// const plumber = require("gulp-plumber");
// const sourcemap = require("gulp-sourcemaps");
// const less = require("gulp-less");
// const postcss = require("gulp-postcss");
// const autoprefixer = require("autoprefixer");
// const sync = require("browser-sync").create();
//
// // Styles
//
// const styles = () => {
//   return gulp.src("source/less/style.less")
//     .pipe(plumber())
//     .pipe(sourcemap.init())
//     .pipe(less())
//     .pipe(postcss([
//       autoprefixer()
//     ]))
//     .pipe(sourcemap.write("."))
//     .pipe(gulp.dest("source/css"))
//     .pipe(sync.stream());
// }
//
// exports.styles = styles;
//
// // Server
//
// const server = (done) => {
//   sync.init({
//     server: {
//       baseDir: 'source'
//     },
//     cors: true,
//     notify: false,
//     ui: false,
//   });
//   done();
// }
//
// exports.server = server;
//
// // Watcher
//
// const watcher = () => {
//   gulp.watch("source/less/**/*.less", gulp.series("styles"));
//   gulp.watch("source/*.html").on("change", sync.reload);
// }
//
// exports.default = gulp.series(
//   styles, server, watcher
// );