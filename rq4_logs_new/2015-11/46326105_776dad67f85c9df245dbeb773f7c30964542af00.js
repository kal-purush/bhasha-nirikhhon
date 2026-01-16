var gulp = require('gulp'),
    connect = require('gulp-connect'),
    history = require('connect-history-api-fallback');

var historyApiFallback = history({});

// Web server for development
gulp.task('server', function () {
  connect.server({
    root: './app',
    hostname: '0.0.0.0',
    port: 8080,
    livereload: function (connect, opt) {
      return [ historyApiFallback ];
    }
  });
});