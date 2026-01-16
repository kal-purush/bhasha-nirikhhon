// https://github.com/shelljs/shelljs
require('shelljs/global')
env.NODE_ENV = 'prod'

let path = require('path')
let config = require('../config')
let ora = require('ora')
let webpack = require('webpack')
let webpackConfig = require('./webpack.prod.conf')

let spinner = ora('building for production...')
spinner.start()

let assetsPath = path.join(config.prod.assetsRoot, config.prod.assetsSubDirectory)
rm('-rf', assetsPath)
mkdir('-p', assetsPath)
cp('-R', 'static/*', assetsPath)

webpack(webpackConfig, (err, stats) => {
    spinner.stop()
    if (err) throw err
    process.stdout.write(stats.toString({
        colors: true,
        modules: false,
        children: false,
        chunks: false,
        chunkModules: false
    }) + '\n')
})