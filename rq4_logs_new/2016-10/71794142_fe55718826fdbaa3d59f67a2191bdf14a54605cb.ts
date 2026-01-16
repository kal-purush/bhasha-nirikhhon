import {Gulpclass, Task, SequenceTask} from 'gulpclass/Decorators';
import gulp = require('gulp');
import gts = require('gulp-typescript');
// import * as browser_sync from 'browser-sync';
import nodemon = require('gulp-nodemon');
import uglify = require('gulp-uglify');
import concat = require('gulp-concat');
import rename = require('gulp-rename');
import del = require('del');


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
export class Gulpfile{

    // tạo biến project của gulp-typescript
    tsProject: gts.Project = gts.createProject('./tsconfig.json');
    // thư mục sẽ chứa các file js đã được compile
    jsDest: string = 'dist/app';

    /**
     * task này sẽ compile các file
     * được khai báo trong phần files của tsconfig
     * và đưa vào thư mục dist/app
     */
    @Task()
    compile(){
        let tsResult = this.tsProject.src().pipe(this.tsProject(gts.reporter.longReporter()));
        return tsResult.js.pipe(gulp.dest(this.jsDest));
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
    nodemon(done: Function){
        let callBackCalled = false;
        return nodemon({script: './dist/app/app.js'}).on('start',() => {
            if(!callBackCalled){
                callBackCalled = true;
                done();
            }
        });
    }

    /**
     * task này sẽ chạy gulp.watch
     * và nó sẽ giám sát sự thay đổi các file ts trong thư mục src
     * và chạy task compile khi có sự thay đổi
     */
    @Task()
    watch(done: Function){
        gulp.watch('src/**/*.ts',['compile']);
        done();
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
    @Task('',['compile'])
    build(){
        return gulp.src('dist/app/**/*.js')
            .pipe(concat('all.js'))
            .pipe(gulp.dest('dist/production'))
            .pipe(rename('all.min.js'))
            .pipe(uglify())
            .pipe(gulp.dest('dist/production'));
    }

    /**
     * task này là default khi chỉ chạy gulp trong command line
     * nó sẽ chạy lần lượt các task compile,nodemon,watch
     */
    @SequenceTask()
    default(){
         return ['compile','nodemon','watch'];
    }
} 