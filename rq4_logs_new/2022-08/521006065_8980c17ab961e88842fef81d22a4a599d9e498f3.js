const gulp 		    = require('gulp'),
    plugins		    = require('gulp-load-plugins')(),
    autoprefixer    = require('autoprefixer'),
    cssnano         = require('cssnano'),
    config          = {
        autoprefixer: {
            enable : true,
            opts: {
                grid: "autoplace"
            }
        },
        sass: {
            minify: false,
            src: 'sass/**/*.{scss,sass}',
            opts: {
                outputStyle: 'expanded',
                sourceMapEmbed: true
            },
            postCssPlugins: [],
            outputName: '',
            dest: 'dist/'
        }
    };

gulp.task('sass', () => {
    if(config.autoprefixer.enable)
        config.sass.postCssPlugins.push(autoprefixer(config.autoprefixer.opts));

    if(config.sass.minify)
        config.sass.postCssPlugins.push(cssnano());

    return gulp.src(config.sass.src)
        .pipe(plugins.sourcemaps.init())
        .pipe(plugins.sass(config.sass.opts).on('error', plugins.sass.logError))
        .pipe(plugins.postcss(config.sass.postCssPlugins))
        .pipe(plugins.sourcemaps.write('.'))
        .pipe(gulp.dest(config.sass.dest));
});

gulp.task('watch', () => {
    gulp.watch(config.sass.src, gulp.series('sass'));
});

gulp.task('default', gulp.series('watch'));