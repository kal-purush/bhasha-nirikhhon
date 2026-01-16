process.env.DISABLE_NOTIFIER = true;
var elixir = require('laravel-elixir');
var gulp = require('gulp');
var concat = require('gulp-concat');
var spritesmith = require('gulp.spritesmith');

require('laravel-elixir-livereload');

elixir(function(mix) {
    //===== combine vendor scripts =====
    mix.scripts([
        'bowers/jquery/jquery.min.js',
        'bowers/bootstrap/dist/js/bootstrap.min.js',
        'bowers/jquery-backstretch/jquery.backstretch.min.js',
    ], 'public/builds/js/vendor.js', 'resources/assets');

    //===== combine manual scripts =====
    mix.scriptsIn('resources/assets/frontend/js', 'public/builds/js/main.js', 'resources/assets');

    //===== combine vendor style =====
    mix.styles([
        'bowers/bootstrap/dist/css/bootstrap.min.css',
        'bowers/font-awesome/css/font-awesome.min.css',
        'bowers/raty/lib/jquery.raty.css',
        
    ], 'public/builds/css/vendor.css', 'resources/assets');

    //===== combine manual style =====
    mix.sass('./resources/assets/frontend/sass/', 'public/builds/css/main.css');
    mix.sass('./resources/assets/frontend/sass_responsive/', 'public/builds/css/responsive.css');

    //===== combine vendor admin scripts =====
    mix.scripts([
        'bowers/jquery/jquery.min.js',
        'bowers/bootstrap/dist/js/bootstrap.min.js',
        'bowers/jquery-backstretch/jquery.backstretch.min.js',
        
    ], 'public/builds/js/vendor.admin.js', 'resources/assets');

    //===== combine manual admin scripts =====
    mix.scriptsIn('resources/assets/backend/js', 'public/builds/js/main.admin.js', 'resources/assets');

    //===== combine vendor admin style =====
    mix.styles([
        'bowers/bootstrap/dist/css/bootstrap.min.css',
    ], 'public/builds/css/vendor.admin.css', 'resources/assets');

    //===== combine manual admin style =====
    mix.sass('./resources/assets/backend/sass/', 'public/builds/css/main.admin.css');

    //==== livereload ====
    mix.livereload([
        'resources/assets/frontend/**/*',
        'resources/assets/backend/**/*',
        'resources/assets/backend/**/**/*.scss',
        'resources/views/**/*',
        'modules/**/views/**/*',
        'modules/**/controllers/**/*'
    ]);
    gulp.src('./resources/assets/bowers/font-awesome/fonts/*.{eot,svg,ttf,woff,woff2}').pipe(gulp.dest('./public/builds/fonts/'));
    gulp.src('./resources/assets/bowers/jquery-smooth-scroll/**/*').pipe(gulp.dest('./public/builds/theme/js/jquery-smooth-scroll/'));
    gulp.src('./resources/assets/bowers/raty/**/*').pipe(gulp.dest('./public/builds/theme/js/raty/'));
    gulp.src('./resources/assets/bowers/OwlCarousel/**/*').pipe(gulp.dest('./public/builds/theme/js/OwlCarousel/'));
    gulp.src('./resources/assets/bowers/jquery-mousewheel/**/*').pipe(gulp.dest('./public/builds/theme/js/jquery-mousewheel/'));
});
gulp.task('admin-theme', function() {
    gulp.src([
        './resources/assets/bowers/iCheck/icheck.min.js',
        './resources/assets/bowers/AdminLTE/dist/js/app.min.js'
    ]).pipe(concat('theme.js')).pipe(gulp.dest('./public/builds/theme/js/'));

    gulp.src([
        './resources/assets/bowers/font-awesome/css/font-awesome.min.css',
        './resources/assets/bowers/Ionicons/css/ionicons.min.css',
        './resources/assets/bowers/iCheck/skins/square/blue.css',
        './resources/assets/bowers/iCheck/skins/flat/blue.css',
        './resources/assets/bowers/iCheck/skins/flat/green.css',
        './resources/assets/bowers/iCheck/skins/flat/red.css',
        './resources/assets/bowers/AdminLTE/dist/css/AdminLTE.min.css',
        './resources/assets/bowers/AdminLTE/dist/css/skins/skin-blue.css',
    ]).pipe(concat('theme.css')).pipe(gulp.dest('./public/builds/theme/css/'));
    
    gulp.src('./resources/assets/bowers/jquery-sortable/**/*').pipe(gulp.dest('./public/builds/theme/js/jquery-sortable/'));
    gulp.src('./resources/assets/bowers/bootstrap-fileinput/**/*').pipe(gulp.dest('./public/builds/theme/js/bootstrap-fileinput/'));
    gulp.src('./resources/assets/bowers/ckeditor/**/*').pipe(gulp.dest('./public/builds/theme/js/ckeditor/'));
    gulp.src('./resources/assets/bowers/select2/**/*').pipe(gulp.dest('./public/builds/theme/js/select2/'));
    gulp.src('./resources/assets/bowers/selectize/**/*').pipe(gulp.dest('./public/builds/theme/js/selectize/'));
    gulp.src('./resources/assets/bowers/font-awesome/fonts/*.{eot,svg,ttf,woff,woff2}').pipe(gulp.dest('./public/builds/theme/fonts/'));
    gulp.src('./resources/assets/bowers/Ionicons/fonts/*.{eot,svg,ttf,woff,woff2}').pipe(gulp.dest('./public/builds/theme/fonts/'));
    gulp.src('./resources/assets/bowers/iCheck/skins/square/*.png').pipe(gulp.dest('./public/builds/theme/css/'));
    gulp.src('./resources/assets/bowers/iCheck/skins/flat/*.png').pipe(gulp.dest('./public/builds/theme/css/'));
});

//---- sprite ----
gulp.task('sprite', function() {
    var spriteData =
        gulp.src('./resources/assets/images/sprite/*.*') // source path of the sprite images
        .pipe(spritesmith({
            imgName: 'sprite.png',
            cssName: 'sprite.css'
        }));

    spriteData.img.pipe(gulp.dest('./public/builds/sprite/')); // output path for the sprite
    spriteData.css.pipe(gulp.dest('./public/builds/sprite/')); // output path for the CSS
});

//---- fonts ----
gulp.task('fonts', function() {
    return gulp.src('./resources/assets/bowers/bootstrap/fonts/*.{eot,svg,ttf,woff,woff2}')
        .pipe(gulp.dest('./public/builds/fonts/'));
});