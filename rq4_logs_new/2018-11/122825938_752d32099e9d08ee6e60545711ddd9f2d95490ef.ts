import path from 'path';
import webpack from 'webpack';

export default (fs, fixture, rules): Promise<any> => {
  const compiler = webpack({
    context: __dirname,
    entry: `./${fixture}`,
    output: {
      path: path.resolve(__dirname, './output/'),
      filename: 'bundle.js',
      library: 'partialComment',
      libraryTarget: 'commonjs2',
    },
    resolve: {
      extensions: ['.js', '.hbs'],
    },
    resolveLoader: {
      modules: ['node_modules'],
    },
    module: {
      rules,
    },
  });

  compiler.outputFileSystem = fs;

  return new Promise((resolve, reject) => {
    compiler.run((err, stats) => {
      if (err) reject(err);

      resolve(stats);
    });
  });
};