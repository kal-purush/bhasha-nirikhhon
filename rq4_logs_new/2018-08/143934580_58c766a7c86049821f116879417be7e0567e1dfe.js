module.exports = {
  css: {
    enabled: true,
    src: [
      'sass/**/*.scss'
    ],
    vendor: [
      'sass/vendor/**/*.css'
    ],
    dest: 'dist/',
    flattenDestOutput: true,
    lint: {
      enabled: true,
      failOnError: true,
      extraSrc: null
    },
    sourceComments: false,
    sourceMapEmbed: false,
    outputStyle: 'compressed',
    autoPrefixerBrowsers: [
      'last 2 versions',
      'IE >= 9'
    ],
    includePaths: [
      './node_modules'
    ],
    sassdoc: {
      enabled: true,
      dest: 'public/sassdoc',
      verbose: false,
      basePath: 'https://bitbucket.org/ietwebdev/article-26-backpack-pattern-lab/src/master/source/sass',
      theme: 'default',
      sort: [
        'file',
        'group',
        'line<'
      ]
    }
  },
  js: {
    enabled: true,
    src: [
      'js/vendor/**/*.js',
      'js/**/*.js'
    ],
    dest: 'dist/',
    destName: 'scripts.js',
    sourceMapEmbed: false,
    uglify: true,
    babel: true,
    preserveLicense: false,
    eslint: {
      enabled: false,
      failAfterError: true,
      extraSrc: [
        '!js/vendor/**/*.js'
      ]
    }
  },
  nodeFiles: {
    enabled: true,
    dir: 'node_modules',
    includePaths: [
      'node_modules/singularitygs/stylesheets',
      'node_modules/breakpoint-sass/stylesheets',
      'node_modules/sass-toolkit/stylesheets',
      'node_modules/sass-burger',
      'node_modules/normalize-scss/sass'
    ]
  },
  bowerFiles: {
    enabled: false,
    dir: 'bower_components'
  },
  browserSync: {
    enabled: false,
    port: 3050,
    watchFiles: null,
    baseDir: './',
    startPath: '/',
    openBrowserAtStart: true,
    browser: [
      'Google Chrome'
    ],
    reloadDelay: 50,
    reloadDebounce: 750
  },
  themeSync: {
    enabled: false,
    newsite: false,
    src: false,
    dest: '../theme/',
    sassSrc: 'source/sass/',
    sassDest: 'sass/1_pattern_lab/',
    jsSrc: 'source/js/',
    jsDest: 'js/pattern_lab/',
    imagesSrc: 'source/images/',
    imagesDest: 'images/'
  },
  patternLab: {
    enabled: false,
    imagesSrc: 'source/images/',
    imagesDest: 'public/images/'
  }
}