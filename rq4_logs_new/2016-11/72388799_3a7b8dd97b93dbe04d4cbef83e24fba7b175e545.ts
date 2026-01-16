import { Gulpclass, Task, SequenceTask } from 'gulpclass/Decorators';
import gulp = require('gulp');
import gts = require('gulp-typescript');
import nodemon = require('gulp-nodemon');
import uglify = require('gulp-uglify');
import concat = require('gulp-concat');
import rename = require('gulp-rename');
import del = require('del');
import childp = require('child_process');
import gutil = require('gulp-util');
import * as bs from 'browser-sync';


/**
 * lớp là lớp chính để chạy gulp
 * muốn thêm task thì thêm vào trong đây
 * 
 * luật chung khi tạo 1 task:
 * - task phải trả về stream
 * - nếu không trả về stream được thì phải nhận vào 1 callback function
 *   và gọi nó khi muốn báo là task đã dừng
 * 
 * vì nếu không trả về hoặc báo là dừng thì task đó sẽ không bao giờ dừng
 * 
 * đọc ở đây để biết thêm:
 * https://github.com/gulpjs/gulp/blob/master/docs/API.md
 */
@Gulpclass()
export class Gulpfile {

    // tạo biến project của gulp-typescript
    tsProject: gts.Project = gts.createProject('./tsconfig.json');
    // thư mục sẽ chứa các file js đã được compile
    jsDest: string = 'dist';
    filesToMove: string[] = [
        './src/config/**/*.*',
        './src/client/index.html',
        './src/systemjs.config.js',
        './src/client/app/**/*.html',
        './src/client/app/**/*.css',
        './src/client/app/assets/**/*.*',
        './src/api/json/**/*.json'
    ];

    /**
     * task này sẽ compile các file
     * được khai báo trong phần files của tsconfig
     * và đưa vào thư mục dist/app
     */
    @Task('',['move'])
    compile() {
        let tsResult = this.tsProject.src().pipe(this.tsProject(gts.reporter.longReporter()));
        return tsResult.js.pipe(gulp.dest(this.jsDest));
    }

    @Task()
    move(){
        return gulp.src(this.filesToMove,{base: './src'}).pipe(gulp.dest('dist'));
    }

    /**
     * task này sẽ chạy nodemon
     * và nodemon này sẽ chạy file app.js
     * và sau đó nodemon sẽ tự động giám sát thư mục hiện tại
     * mà nó được gọi là NODEJS-API-SECURIRY này và tự động restart lại khi có sự thay đổi
     * 
     * lên đây để biết thêm chi tiết https://github.com/remy/nodemon 
     */
    @Task()
    nodemon(done: Function) {
        let callBackCalled = false;
        return nodemon({ script: './dist/server.js', watch: ['dist/api/**/*.js'] }).on('start', () => {
            if (!callBackCalled) {
                callBackCalled = true;
                done();
            }
        });
    }

    @Task()
    serve(done){
        bs.init({
            proxy: 'localhost:8080'
        },(err,bs) => {
            if(err){
                console.error(err.message);
            }
        });

        gulp.watch('src/client/**/*.html',['move',bs.reload])
        gulp.watch('src/client/**/*.css',['move',bs.reload])
        gulp.watch(['src/client/**/*.ts'], ['compile',bs.reload]);
        gulp.watch(['src/api/**/*.ts'], ['compile',bs.reload]);

        done();
    }

    @Task('', ['compile'])
    test(done) {
        let stdout = '';
        let stderr = '';
        let child = childp.exec('npm test', { cwd: process.cwd() });

        child.stdout.on('data', (data) => {
            stdout += data;
        });

        child.stderr.on('data', (data) => {
            stderr += data;
        })

        child.on('close', (code) => {
            gutil.log('exit with code', code);
            gutil.log(gutil.colors.blue(stdout));
            gutil.log(gutil.colors.red(stderr));
            done();
        })
    }

    /**
     * task này phụ thuộc vào 2 task clean và compile
     * chỉ khi clean và compile chạy xong thì task này mới chạy
     * 
     * task này có nhiệm vụ là lấy tất cả các file js
     * trong thư mục src và nối lại thành file all.js
     * bỏ vào thư mục dist/production, và sau đó tạo file all.min.js
     * và minify file all.js vào file đó và bỏ vào thư mục dist/production 
     */
    @Task('', ['compile'])
    build() {
        return gulp.src('dist/app/**/*.js')
            .pipe(concat('all.js'))
            .pipe(gulp.dest('dist/production'))
            .pipe(rename('all.min.js'))
            .pipe(uglify())
            .pipe(gulp.dest('dist/production'));
    }

    @Task()
    delDist(){
        return del(this.jsDest);
    }

    /**
     * task này là default khi chỉ chạy gulp trong command line
     * nó sẽ chạy lần lượt các task compile,nodemon,watch
     */
    @SequenceTask()
    default() {
        return ['compile','nodemon','serve'];
    }
} 