// note that we are using @heroku-cli/command instead of @oclif/command
// this inherits from @oclif/command but extends it with Heroku-specific functionality
import {Command, flags} from '@heroku-cli/command'
import {cli} from 'cli-ux'
import {HTTP} from 'http-call'

import Build from './build'
import Export from './export'

const fs = require('fs')
const Sanbashi = require('../../sanbashi')
const streamer = require('../../streamer')

type ReleaseBody = {
  addon_plan_names: [string],
  app: {
    id: string,
    name: string
  },
  created_at: Date,
  description: string,
  status: 'succeeded' | 'pending' | 'failded',
  id: string,
  slug?: string,
  updated_at: Date,
  user: {
    email: string,
    id: string
  },
  version: number,
  current: boolean,
  output_stream_url?: string,
}

export default class Push extends Command {
  static description = 'Deploy an app'
  static examples = [`
$ heroku _container:push`,
  ]
  static flags = {
    remote: flags.remote(),
    app: flags.app({required: true}),
    'skip-stack-pull': flags.boolean()
  }

  async run() {
    const {flags} = this.parse(Push)

    let processType = 'web'
    let registryImage = `registry.heroku.com/${flags.app}/${processType}`

    // build
    if (!fs.existsSync(`${flags.app}.slug`)) {
      cli.styledHeader(`No slug detected, building slug for ${flags.app}`)
      let buildArgs = []
      if (flags['skip-stack-pull']) {
        buildArgs.push('--skip-stack-pull')
      }
      await Build.run(buildArgs)
    }

    // export
    let exportArgs = [`--tag=${registryImage}`]
    if (flags['skip-stack-pull']) {
      exportArgs.push('--skip-stack-pull')
    }
    cli.styledHeader(`Export docker image ${processType} for ${flags.app}`)
    await Export.run(exportArgs)

    // push
    cli.styledHeader(`Pushing ${processType} for ${flags.app}`)
    await Sanbashi.pushImage(registryImage)

    // release
    let imageID = await Sanbashi.imageID(registryImage)
    let updateData = [{
      type: processType,
      docker_image: imageID
    }]

    cli.styledHeader(`Releasing image ${processType} to ${flags.app}`)
    cli.action.start('Releasing')
    await this.heroku.patch(`/apps/${flags.app}/formation`, {
      body: {updates: updateData},
      headers: {
        Accept: 'application/vnd.heroku+json; version=3.docker-releases'
      }
    })
    cli.action.stop()

    let release: ReleaseBody = await this.heroku.request(`/apps/${flags.app}/releases`, {
      partial: true,
      headers: {Range: 'version ..; max=2, order=desc'}
    }).then((releases: HTTP<any>): ReleaseBody => releases.body[0])

    if (release.output_stream_url && release.status === 'pending') {
      cli.log('Running release command...')
      await streamer(release.output_stream_url, process.stdout)
    }
  }
}