import {join} from 'path';
import {ASSETS_SRC, TMP_DIR} from '../config';

export = function buildSass(gulp, plugins) {
  return function () {
      return gulp.src(join(ASSETS_SRC, 'main.scss'))
        .pipe(plugins.sass().on('error', plugins.sass.logError))
        .pipe(gulp.dest(TMP_DIR));
  };
}