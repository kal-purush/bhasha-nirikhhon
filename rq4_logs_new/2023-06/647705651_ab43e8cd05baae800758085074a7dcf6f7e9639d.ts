import * as fs from 'node:fs'

import { FileBase } from 'projen'

import type { FileBaseOptions, Project } from 'projen'
/**
 * Options for `TextFile`.
 */
export interface TextFileOptions extends FileBaseOptions {
  readonly templatePath: string
}

export class ScriptFile extends FileBase {
  private readonly templatePath: string
  constructor(project: Project, filePath: string, options: TextFileOptions) {
    super(project, filePath, options)
    this.templatePath = options.templatePath
  }

  protected synthesizeContent(): string {
    return `// ${this.marker} \n ${fs.readFileSync(this.templatePath, {
      encoding: 'utf8',
    })} 
    `
  }
}