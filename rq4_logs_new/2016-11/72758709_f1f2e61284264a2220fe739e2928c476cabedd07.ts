import paths from '../paths-ts';

export default [
  {
    test: /\.ts$/,
    include: [paths.src],
    loader: 'awesome-typescript-loader'
  },
  {
    test: /\.scss$/,
    include: paths.src,
    loaders: ['style', 'css', 'sass']
  },
  {
    test: /\.css$/,
    loader: 'style!css!postcss'
  },
  {
    test: /\.json$/,
    loader: 'json'
  },
  {
    test: /\.(ico|jpg|jpeg|png|gif|eot|otf|webp|svg|ttf|woff|woff2)(\?.*)?$/,
    loader: 'file',
    query: {
      name: 'static/media/[name].[hash:8].[ext]'
    }
  }
];