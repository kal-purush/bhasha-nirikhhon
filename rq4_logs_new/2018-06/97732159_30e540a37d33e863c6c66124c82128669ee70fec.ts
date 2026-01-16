import { assert } from 'chai'
import * as path from "path";
import * as fs from "fs";

require('source-map-support').install()

const helpers = require('yeoman-test')

const appGeneratorPath = path.resolve('generators/app')
const tempPath = path.resolve('test/temp')
describe('yeoman', function () {

  it('can generate a minimal project', async function () {
    await helpers.run(appGeneratorPath)
      .inDir(tempPath) // This globally modifies the working directory and does not set it back.  Yeah.
      // .withOptions({ foo: 'bar' })
      // .withArguments(['name-x'])
      // .withPrompts({ coffee: false })
      .withLocalConfig({ lang: 'en' })

    console.log('Hello World')
    assert(fs.statSync('package.json').isFile())
  })

})