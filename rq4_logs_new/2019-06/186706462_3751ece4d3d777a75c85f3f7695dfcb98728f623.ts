import { AngularCompilerPlugin, PLATFORM } from '@ngtools/webpack';
import { join, resolve } from 'path';

const root = resolve(__dirname, '../..');

export const angularCompilerPluginFactory: (
  env: any,
  argv: any
) => AngularCompilerPlugin = (env, argv) => {
  return new AngularCompilerPlugin({
    platform: PLATFORM.Browser,
    entryModule: join(root, 'src/app/app.module#AppModule'),
    mainPath: join(root, 'src/main'),
    tsConfigPath: resolve(root, 'src/tsconfig.app.json'),
    hostReplacementPaths: env.production
      ? {
          'environments/environment.ts': 'environments/environment.prod.ts',
        }
      : undefined,
    sourceMap: env.production ? false : true,
    skipCodeGeneration: false,
    directTemplateLoading: true,
  });
};