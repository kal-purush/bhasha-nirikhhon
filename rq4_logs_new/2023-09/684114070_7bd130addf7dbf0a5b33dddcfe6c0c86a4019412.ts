import { createTreeWithEmptyWorkspace } from '@nx/devkit/testing';
import { Tree, readProjectConfiguration, addProjectConfiguration } from '@nx/devkit'

import { handlerGenerator } from './generator';
import { HandlerGeneratorSchema } from './schema';

describe('handler generator', () => {
  let tree: Tree;
  const options: HandlerGeneratorSchema = { name: 'test', project: 'stack' };

  beforeEach(() => {
    tree = createTreeWithEmptyWorkspace()
    addProjectConfiguration(tree, options.project, {
      root: `stacks/${options.project}`,
      projectType: 'application',
      sourceRoot: `stacks/${options.project}/src`,
      targets: {},
      tags: ['stacks']
    });
  });

  it('should run successfully', async () => {
    await handlerGenerator(tree, options);
    const config = readProjectConfiguration(tree, 'stack');
    expect(config).toBeDefined();
  });
});