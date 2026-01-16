import * as Generator from 'yeoman-generator'
import { CommonContext, VineyardAnswers } from "./types";

function copyBundle(generator: Generator, path: string, context: any) {
  generator.fs.copyTpl(
    generator.templatePath(path),
    generator.destinationPath(''),
    context
  )
}

async function gatherUserInput(generator: Generator): Promise<VineyardAnswers> {
  const answers = await generator.prompt([
    {
      type: 'input',
      name: 'projectName',
      message: 'The project name',
      default: generator.appname
    },
    {
      type: 'input',
      name: 'description',
      message: 'Project description',
      default: ''
    },
  ])

  return answers as VineyardAnswers
}

function newCommonContext(answers: VineyardAnswers): CommonContext {
  return {
    packageConfig: {
      projectName: answers.projectName,
      description: answers.description,
      dependencies: {
        "@types/node": "^7.0.16",
        "source-map-support": "0.5.0",
        "typescript": "2.8.4"
      },
      devDependencies: {
        "@types/mocha": "^2.2.43",
        "mocha": "^3.2.0",
        "tslint": "^5.8.0",
        "tslint-config-standard": "^7.0.0"
      }
    }
  }
}

export async function main(generator: Generator) {
  const answers = await gatherUserInput(generator)
  const context = newCommonContext(answers)
  copyBundle(generator, 'common', context)
}