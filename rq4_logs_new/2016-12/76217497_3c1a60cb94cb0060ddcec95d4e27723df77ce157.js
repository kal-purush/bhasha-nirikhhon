/**
 * Created by apple on 16/12/13.
 */
module.exports = {

    entry: {
        animation: './src/animation.js'
    },
    output: {
        path: __dirname + '/build',
        filename: '[name].js',
        library: 'animation',
        libraryTarget: 'umd'
    }
};