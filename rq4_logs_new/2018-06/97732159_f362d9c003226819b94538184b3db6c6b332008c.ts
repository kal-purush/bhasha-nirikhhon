import * as Generator from "yeoman-generator";
import { UserAnswers } from "./types";

export async function gatherUserInput(generator: Generator): Promise<UserAnswers> {
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

    {
      type: 'confirm',
      name: 'includeLawn',
      message: 'Include Vineyard Lawn (for web services)',
      default: false
    },

    {
      type: 'confirm',
      name: 'includeUsers',
      message: 'Include Vineyard Users (for user authentication and management)',
      default: false,
      when: (a: any) => a.includeLawn
    },

    {
      type: 'confirm',
      name: 'includeMinotaur',
      message: 'Include Vineyard Minotaur (for blockchain monitoring)',
      default: false
    },
  ])

  return answers as UserAnswers
}