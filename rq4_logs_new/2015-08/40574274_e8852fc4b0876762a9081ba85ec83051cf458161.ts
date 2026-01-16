/// <reference path="../typings/node-telegram-bot-api/TelegramBot.d.ts"/>
/// <reference path="../typings/node/node.d.ts"/>

import {resolve} from 'path'
import {TelegramTypedBot as Bot, IServerOptions, BotAction} from './bot/telegram-typed-bot';
import {loadStudents, loadAssistents} from './models/parser';
import {Assistent, Student, Person} from './models/models';

export import t = require('./bot/telegram-bot-typings')

export interface AyudanteBOTOptions  {
  github: string
  tokens: {telegram: string, github: string}
  server: IServerOptions
}

interface ISearchOptions {
    text?: string
}

interface ISearchStudentOptions extends ISearchOptions {
  section?: number
  quitted?: boolean
}

interface ISearchAssistentOptions extends ISearchOptions {
  area?: string
  level?: string
}

export class AyudanteBOT extends Bot {
  github: string
  students: Student[]
  assistents: Assistent[]

  constructor(options: AyudanteBOTOptions) {
    super(options.tokens.telegram,options.server)

    this.github = options.github
    this.students = loadStudents(resolve('.', 'data', 'students.csv'))
    this.assistents = loadAssistents(resolve('.', 'data', 'assistents.csv'))
  }

  get githubUrl(): string {
    return 'https://github.com/' + this.github;
  }

  get syllabusUrl(): string {
    return this.githubUrl + '/syllabus'
  }

  get privateUrl(): string {
    return this.githubUrl + '/private'
  }

  get starterUrl(): string {
    return this.githubUrl + '/starter'
  }

  personGithubURLinOrganization(person: Person): string {
    return this.githubUrl + '/' + person.github;
  }

  searchStudent(o: ISearchStudentOptions): Student[] {
    const filtered = this.students.filter(a => {
      if (o.section !== undefined && a.section !== o.section) return false
      if (o.quitted !== undefined && a.quitted !== o.quitted) return false
      return true
    })

    return this.searchPerson(filtered, o.text)
  }

  searchAssistent(o: ISearchAssistentOptions): Assistent[] {
    const filtered = this.assistents.filter(a => {
      if (o.area !== undefined && a.area.toLowerCase() !== o.area.toLowerCase()) return false
      if (o.level !== undefined && a.level.toLowerCase() !== o.level.toLowerCase()) return false
      return true
    })

    return this.searchPerson(filtered, o.text)
  }

  searchPerson(collection: Person[], text?: string): any[] {
    if (!text || text === '') return collection

    const containsInsensitive = (container, s) => {
      return container.toLowerCase().indexOf(s.toLowerCase()) !== -1
    }

    return collection.filter(s => {
      return [s.name, s.uc, s.github].some(field => {
        return containsInsensitive(field, text)
      })
    })
  }

  printableStudent(s): string {
    const repo = this.personGithubURLinOrganization(s);
    const text =
`${s.name} (${s.uc})
Sec. ${s.section}
Github: ${s.githubURL}
Repo: ${repo}`;
    return text;
  }

  printableAssistent(s): string {
    const text =
`${s.name} (${s.uc}) [${ s.level} ${s.area}]
Teléfono: ${s.phoneWithArea}
Github: ${s.githubURL}`;
    return text;
  }

  printableStudents(students: Student[]): string {
    return students.map(this.printableStudent).join('\n--------------------------\n');
  }

  printableAssistents(assistents: Assistent[]): string {
    return assistents.map(this.printableAssistent).join('\n--------------------------\n');
  }
}