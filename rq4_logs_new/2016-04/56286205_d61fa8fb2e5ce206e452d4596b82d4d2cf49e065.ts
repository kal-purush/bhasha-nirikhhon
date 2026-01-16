import * as gulp from 'gulp';
import * as tsc from 'gulp-typescript';
import * as sourcemaps from 'gulp-sourcemaps';
import * as nodemon from 'gulp-nodemon';

gulp.task('script', () => {
    let tsProject = tsc.createProject('tsconfig.json');
 
    return tsProject
        .src()
        .pipe(sourcemaps.init())
        .pipe(tsc(tsProject))
        .pipe(sourcemaps.write())
        .pipe(gulp.dest('dist'));
});

gulp.task('nodemon', ['script'], () => {
    nodemon({
        script: 'dist/server/app.js',
        ext: 'js',
        env: { 'NODE_ENV': 'development' }
    });
});

gulp.task('watch', ['nodemon'], () => {
    gulp.watch('src/**/*.ts', ['script']);
});

gulp.task('default', ['watch']);