// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
import { javascript, JsonPatch } from 'projen'
import { TypescriptConfigExtends } from 'projen/lib/javascript/typescript-config'

import type { JsiiProject } from 'projen/lib/cdk'
import type {
  TypeScriptProject,
  TypeScriptProjectOptions,
} from 'projen/lib/typescript'

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export function getSharedOptions() {
  return {
    defaultReleaseBranch: 'main',
    packageManager: javascript.NodePackageManager.NPM,
    projenrcTs: true,
    docgen: false,
    eslint: true,
    jest: false,
    githubOptions: { mergify: false, pullRequestLint: false },
    codeCov: false,
    vscode: false,
    buildWorkflow: false,
    prettier: false,
    pullRequestTemplate: false,
    projenrcTsOptions: { swc: true },
    tsconfigDev: {
      compilerOptions: {},
      extends: TypescriptConfigExtends.fromPaths([
        'chetzof-lint-config/tsconfig/tsconfig.json',
      ]),
    },
    tsconfig: {
      compilerOptions: {},
      extends: TypescriptConfigExtends.fromPaths([
        'chetzof-lint-config/tsconfig/tsconfig.json',
      ]),
    },
  } satisfies Partial<TypeScriptProjectOptions>
}

export function applyProjectChanges(
  project: JsiiProject | TypeScriptProject,
): void {
  project.eslint?.addExtends(
    './node_modules/chetzof-lint-config/eslint/index.js',
  )
  project.gitignore.exclude('/.idea')
  project.addDevDeps('chetzof-lint-config', 'prettier')
  project
    .tryFindObjectFile('.eslintrc.json')
    ?.patch(JsonPatch.replace('/rules', {}))
  project.tryFindObjectFile(project.tsconfigDev.fileName)?.patch(
    JsonPatch.replace('/compilerOptions', {
      baseUrl: './',
      rootDir: 'src',
      outDir: 'lib',
    }),
  )

  if (project.tsconfig?.fileName) {
    project.tryFindObjectFile(project.tsconfig.fileName)?.patch(
      JsonPatch.replace('/compilerOptions', {
        baseUrl: './',
        rootDir: 'src',
        outDir: 'lib',
      }),
    )
  }

  project.package.addField(
    'prettier',
    'chetzof-lint-config/prettier/.prettierrc.js',
  )
}