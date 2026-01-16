import * as path from 'path';

export default {
  extensions: ['.tsx', '.ts', '.js'],
  alias: {
    components: path.resolve(path.join(__dirname, '../../'), 'src/components/'),
    pages: path.resolve(path.join(__dirname, '../../'), 'src/pages/'),
    modules: path.resolve(path.join(__dirname, '../../'), 'src/modules/'),
    utils: path.resolve(path.join(__dirname, '../../'), 'src/utils/'),
    common: path.resolve(path.join(__dirname, '../../'), 'src/common/'),
    api: path.resolve(path.join(__dirname, '../../'), 'src/api/'),
    assets: path.resolve(path.join(__dirname, '../../'), 'src/assets/'),
    core: path.resolve(path.join(__dirname, '../../'), 'src/core/'),
    store: path.resolve(path.join(__dirname, '../../'), 'src/store/'),
  },
};