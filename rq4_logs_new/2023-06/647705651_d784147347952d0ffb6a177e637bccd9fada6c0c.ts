import { cdk, javascript, JsonPatch } from 'projen'

const project = new cdk.JsiiProject({
  author: 'Vlad Cos',
  authorAddress: 'vcosvic@gmail.com',
  defaultReleaseBranch: 'main',
  jsiiVersion: '~5.0.0',
  name: 'jsiproj',
  packageName: 'loll',
  packageManager: javascript.NodePackageManager.NPM,
  projenrcTs: true,
  repositoryUrl: 'https://github.com/vcosvic/jsiproj.git',
  peerDeps: ['projen'],
  deps: ['projen'],
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
  devDeps: ['chetzof-lint-config', 'prettier'],
  // eslintOptions: {},
})
project.eslint.addExtends('./node_modules/chetzof-lint-config/eslint/index.js')
project.gitignore.exclude('/.idea')
project
  .tryFindObjectFile('.eslintrc.json')
  .patch(JsonPatch.replace('/rules', {}))
project.package.addField(
  'prettier',
  'chetzof-lint-config/prettier/.prettierrc.js',
)
project.synth()