/// <reference path="typings/tsd.d.ts" />

import gulp = require("gulp");
import del = require("del");
import browserSync = require("browser-sync");

var $ = require("gulp-load-plugins")();
var runSequence = require("run-sequence");

class Tasks {
    [id: string]: gulp.ITaskCallback;

    private build(): NodeJS.ReadWriteStream {
        return this.compile();
    }

    private clean(): void {
        del("./dest/**/*", void 0);
        del("./dev/**/*", void 0);
    }

    private compile(): NodeJS.ReadWriteStream {
        return gulp.src(["./src/ts/*.ts"])
                   .pipe($.sourcemaps.init())
                   .pipe($.typescript({
                       noEmitOnError: true,
                       noImplicitAny: true,
                       target: "ES5",
                       sortOutput: true
                   })).js
                   .pipe($.sourcemaps.write("."))
                   .pipe(gulp.dest("./dev/js/"))
                   .pipe($.size({ title: "compile" }));
    }

    private minify(): NodeJS.ReadWriteStream {
        return gulp.src(["./dev/js/*.js", "!./dev/js/*.min.js"])
                   .pipe($.uglify({ preserveComments: "some" }))
                   .pipe($.rename({ extname: ".min.js" }))
                   .pipe(gulp.dest("./dest/js/"))
                   .pipe($.size({ title: "minify" }));
    }

    private html(): NodeJS.ReadWriteStream {
        var assets = $.useref.assets();
        return gulp.src(["./index.html"])
                   .pipe(assets)
                   .pipe($.if("*.js", $.uglify()))
                   .pipe($.if("*.css", $.csso()))
                   .pipe(assets.restore())
                   .pipe($.useref())
                   .pipe(gulp.dest("./dest/"))
                   .pipe($.size({ title: "html" }));
    }

    private styles(): NodeJS.ReadWriteStream {
        return gulp.src(["./src/css/style.css"])
                   .pipe(gulp.dest("./dev/css/"));
    }

    private copy(): NodeJS.ReadWriteStream {
        return gulp.src(["CNAME"])
                   .pipe(gulp.dest("./dest/"))
                   .pipe($.size({ title: "copy" }));
    }

    private lint(): NodeJS.ReadWriteStream {
        return gulp.src("./src/ts/*.ts")
                   .pipe($.tslint())
                   .pipe($.tslint.report("verbose"));
    }

    private lint_noemit(): NodeJS.ReadWriteStream {
        return gulp.src("./src/ts/*.ts")
                   .pipe($.tslint())
                   .pipe($.tslint.report("verbose"), {
                       emitError: false
                   });
    }

    private serve(): void {
        browserSync({
            notify: false,
            logPrefix: "WSK",
            server: ["."],
            files: ["index.html", "./dev/css/*.css", "./dev/js/*.js"]
        });
        gulp.watch(["./src/**/*.css"], ["styles"]);
        gulp.watch(["./src/ts/*.ts"], ["build", "lint:noemit"]);
    }

    private serve_dest(): void {
        browserSync({
            notify: false,
            logPrefix: "WSK",
            server: "dest"
        });
    }

    public static register(): void {
        gulp.task("default", ["clean"], cb => runSequence("styles", "lint", "build", ["html", "copy"], "minify", cb));

        var instance = new Tasks();
        for (var task in instance) {
            gulp.task((<string>task).replace("_", ":"), instance[task].bind(instance));
        }
    }
}

Tasks.register();