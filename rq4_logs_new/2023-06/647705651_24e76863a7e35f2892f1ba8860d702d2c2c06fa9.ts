import { resolve } from 'app-root-path'
import { Component, SampleDir } from 'projen'

import { ScriptFile } from '../script-file'

import type { TypeScriptProject } from '../projects/typescript'
import type { Project } from 'projen'

export interface VitestOptions {}
export class Vitest extends Component {
  /**
   * Returns the singletone Jest component of a project or undefined if there is none.
   */
  public static of(project: Project): Vitest | undefined {
    return project.components.find(
      (c: Component): c is Vitest => c instanceof Vitest,
    )
  }

  constructor(project: TypeScriptProject, options: VitestOptions = {}) {
    super(project)
    project.package.addDevDeps('vitest', 'vite-tsconfig-paths')

    project.tsconfigDev.addInclude('vitest.config.ts')
    project.tsconfigDev.addInclude('tests/**/*.ts')
    new ScriptFile(project, 'vitest.config.ts', {
      sourcePath: resolve('templates/vitest.config.ts'),
      readonly: true,
      marker: true,
    })
    new SampleDir(project, 'tests', {
      sourceDir: resolve('templates/tests'),
    })
  }
  preSynthesize() {
    super.preSynthesize()
    this.project.testTask.exec('vitest')
  }
}