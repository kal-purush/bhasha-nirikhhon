var gulp = require('gulp');
var sass = require('gulp-sass');
var browserSync = require('browser-sync').create();
 


gulp.task('task-name', function(){
//Stuff Here
      console.log('Keep Summer Safe');
});

gulp.task('sass', function(){
  return gulp.src('sass/*.scss')
    .pipe(sass()) // Using gulp-sass
    .pipe(gulp.dest('./'))
     .pipe(browserSync.reload({
      stream: true
    }))
});
 

gulp.task('watch', ['browserSync', 'sass'], function(){
  gulp.watch('sass/*.scss', ['sass']);
	 gulp.watch('*.html', browserSync.reload); 
  gulp.watch('wp-content/themes/ensixdesign/js/*.js', browserSync.reload); 
});

 

gulp.task('browserSync', function() {
 browserSync.init({
        server: {
            baseDir: "./"
        }
    });
})

 