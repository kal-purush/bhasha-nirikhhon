import util = require('gulp-util');
import chalk = require('chalk');
import {join} from 'path';
import {SERVER_DEST} from './config';

export = function server_test(gulp, plugins, option) {
    return function () {
        util.log(chalk.bgBlue('Starting server_test...'));

        return gulp.src([join(SERVER_DEST, 'test/**/*.js')])
            .pipe(plugins.mochaCo({
                reporter: 'spec'
            }))
            .once('end', function () {
                process.exit();
            });
    };
}