/* eslint-disable no-console */
import { cwd } from 'process'
import webpack, { Stats } from 'webpack'
import prepareWebPackConfig from './../utils/prepareWebPackConfig'
import execPromise from './../utils/execPromise'
import gzipSize from 'gzip-size'
import { readFileSync } from 'fs'
import { join } from 'path'

export const prepareInstallCommand = (name: string, version: string) => {
  const installPath = `${cwd()}/installs/${name}@${version}`
  const createPath = `mkdir -p ${installPath} && cd ${installPath}`
  const installCommand = `yarn add ${name}@${version} -E -s --no-lockfile --cwd ./`
  const indexFile = `echo "require('${name}')" > index.js`

  const mergedCommands = [createPath, indexFile, installCommand].join(' && ')

  return execPromise(mergedCommands)
}

export const preparePackageVersionStats = (
  name: string,
  version: string
): Promise<Stats> => {
  const entry = `installs/${name}@${version}`

  const webpackConfig = prepareWebPackConfig(entry)

  return new Promise((resolve, reject) => {
    webpack(webpackConfig).run(async (err, stats) => {
      if (err || stats.hasErrors()) {
        const { errors: statsErr } = stats.hasErrors()
          ? stats.toJson()
          : { errors: {} }
        return reject({ err, statsErr })
      }
      resolve(stats)
    })
  })
}

export const deletePath = (path: string) => {
  return execPromise(`rm -rf ${path}`)
}

export const getGzipSizes = (path: string) => {
  return gzipSize(readFileSync(path, 'utf-8'))
}

export const getMinifiedSizes = (
  versions: string[],
  versionsStats: Stats[]
) => {
  return versions.map((version: string, index: number) => {
    const stats = versionsStats[index]
    const { assets } = stats.toJson()

    const { outputOptions } = stats.compilation
    const { path, filename } = outputOptions
    const prodFile = join(path, filename)

    const minified = assets?.find(
      ({ name }) => decodeURIComponent(name) === 'prod.js'
    )

    return {
      version,
      minified: minified?.size,
      path,
      prodFile,
    }
  })
}