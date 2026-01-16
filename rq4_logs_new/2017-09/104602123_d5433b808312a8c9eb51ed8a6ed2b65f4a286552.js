import gulp from 'gulp';
import spritesmith from 'gulp.spritesmith';
import merge from 'merge-stream';
import clean from 'gulp-clean';
import buffer from 'vinyl-buffer';
import imagemin from 'gulp-imagemin';
import pngquant from 'imagemin-pngquant';

gulp.task('sprite', ()=> {
	var spriteData = gulp.src(['app/sprites/*.png','!app/sprites/*@3x.png'])
	.pipe(spritesmith({
		imgName: 'sprite.png',
    	imgPath: "../images/sprite.png",
		cssName: 'sprite.css',
		cssFormat: 'css',
    	padding: 20,
    	cssTemplate: 'handlebarsStr.css.handlebars'
	}));
	var imgStream = spriteData.img
	// DEV: We must buffer our stream into a Buffer for `imagemin`
	.pipe(buffer())
	.pipe(imagemin({
        optimizationLevel: 3,
		progressive: true, 
		interlaced: true,
		use: [pngquant()]
    }))
	.pipe(gulp.dest('app/images/'));
	var cssStream = spriteData.css
	.pipe(gulp.dest('app/styles/'));
	return merge(imgStream, cssStream);
});

gulp.task("clean", ()=> {
    return gulp.src(['app/images/sprite.png'], {read: false})
    .pipe(clean());
});

gulp.task('default', ['clean'], ()=> {
	gulp.start('sprite');
});

// gulp.watch('app/sprites/*.*', ['sprite']).on('change', (event)=> {
// 	console.log('File ' + event.path + ' was ' + event.type + ',running tasks...[image]');
// });
	






