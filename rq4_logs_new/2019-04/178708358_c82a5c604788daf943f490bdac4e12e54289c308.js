import {join} from 'path'

const include = join(__dirname, 'src')
const isProd = process.argv.indexOf('-p') !== -1;

export default {
  mode: isProd ? 'production' : 'development',
  entry: './src/index',
  output: {
    path: join(__dirname, 'dist'),
    libraryTarget: 'umd',
    library: 'starWarsNames',
  },
  devtool: 'source-map',
  module: {
    rules: [
      {test: /\.js$/, loader: 'babel-loader', include},
    ]
  }
}