'use strict';

module.exports = {
  copy: {
    gosha: {
      files: [{
        expand: true,
        src: [
          '*.html',
          'css/**',
          'img/**',
          'js/**'
        ],
        dest: 'gosha',
      }]
    }
  },

  clean: {
    gosha: [
      'gosha/img/README',
      'gosha/js/README',
      'gosha/css/README'
    ]
  },

  lintspaces: {
    codestyle: {
      src: [
        '*.html',
        'js/*.js',
        'less/*.less',
        'sass/*.sass',
        'sass/*.scss'
      ],
      options: {
        editorconfig: '.editorconfig'
      }
    }
  }
};