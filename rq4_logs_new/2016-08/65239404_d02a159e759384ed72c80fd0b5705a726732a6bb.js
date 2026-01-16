module.exports = function (grunt) {
  'use strict'
  require('load-grunt-tasks')(grunt, {
    pattern: 'grunt-*',
    config: './package.json',
    scope: 'devDependencies'
  })

  var semver = require('semver')
  var exec = require('child_process').exec
  var async = require('async')
  var dir = null
  var basePath = grunt.option('basePath') || './'
  var packageJson = grunt.file.readJSON('package.json')
  var versions = packageJson.version.match(/^(\d+)\.(\d+)\.(\d+)(\-pre\.(\d+))?$/)
  var major = versions[ 1 ]
  var minor = versions[ 2 ]
  var patch = versions[ 3 ]
  var pre = versions[ 5 ]
  var preVersion = packageJson.version
  var patchVersion = [ major, minor, patch ].join('.')
  var minorVersion = [ major, minor ].join('.')
  var tagPrefix = 'v'
  var preTag = tagPrefix + preVersion
  var patchTag = tagPrefix + patchVersion
  var minorTag = tagPrefix + minorVersion
  var latestTag = 'latest'
  var maintenanceBranch = 'v' + minorVersion + '.x'
  var origin = grunt.option('remote') || 'origin'
  var master = grunt.option('source-branch') || 'master'

  var releaseMinor = [
    'shell:confirmOnMasterBranch',
    'shell:confirmNoUntrackedFiles',
    'shell:confirmNoModifiedFiles',
    'gitfetch:tags',
    'gitpull:origin',
    'bump:minor',
    'gittag:patch',
    'gittag:minor',
    'gittag:latest',
    'gitcheckout:maintenance',
    'bump:prepatch',
    'gitpush:maintenance',
    'gitcheckout:master',
    'bump:preminor',
    'gitpush:master',
    'gitpush:patchTag',
    'gitpush:minorTag',
    'gitpush:latestTag'
  ]

  var releasePatch = [
    'shell:confirmOnMaintenanceBranch',
    'shell:confirmNoUntrackedFiles',
    'shell:confirmNoModifiedFiles',
    'gitfetch:tags',
    'gitpull:origin',
    'bump:patch',
    'gittag:patch',
    'gittag:minor',
    'bump:prepatch',
    'gitpush:maintenance',
    'gitpush:patchTag',
    'gitpush:minorTag',
    'advanceLatestTagIfNecessary' // creates & pushes latestTag only if necessary
  ]

  var releasePre = [
    'shell:confirmNoUntrackedFiles',
    'shell:confirmNoModifiedFiles',
    'gitpull:origin',
    'gittag:pre',
    'bump:prerelease',
    'gitpush:current',
    'gitpush:preTag'
  ]

  var config = {
    pkg: packageJson,
    basePath: basePath,
    dir: dir,
    bump: {
      options: {
        files: './package.json',
        commit: true,
        commitMessage: 'Rev to v%VERSION%',
        commitFiles: [ '-a' ],
        push: false,
        pushTo: origin,
        prereleaseName: 'pre',
        createTag: false,
        pushTags: false
      }
    },
    gitpush: {
      master: { options: { remote: origin, branch: master } },
      maintenance: { options: { remote: origin, branch: maintenanceBranch, upstream: true } },
      current: { options: { remote: origin, } },
      preTag: { options: { remote: origin, branch: preTag } },
      patchTag: { options: { remote: origin, branch: patchTag } },
      minorTag: { options: { remote: origin, branch: minorTag, force: true } },
      latestTag: { options: { remote: origin, branch: latestTag, force: true } }
    },
    gittag: {
      pre: { options: { tag: preTag } },
      patch: { options: { tag: patchTag } },
      minor: { options: { tag: minorTag, force: true } },
      latest: { options: { tag: latestTag, force: true } },
    },
    gitpull: {
      origin: { options: { remote: origin } }
    },
    gitfetch: {
      tags: { options: { remote: origin, tags: true } }
    },
    gitcheckout: {
      master: { options: { branch: master } },
      maintenance: { options: { branch: maintenanceBranch, create: true } }
    },
    shell: {
      confirmOnMasterBranch: {
        command: "[ $(git status | head -n 1 | awk '{ print $3 }') == '" + master + "' ]"
      },
      confirmOnMaintenanceBranch: {
        command: "[[ $(git status | head -n 1 | awk '{ print $3 }') =~ ^v[0-9]+\\.[0-9]+\\.x$ ]]"
      },
      confirmNoUntrackedFiles: {
        command: '[ -z "$(git status -s)" ]'
      },
      confirmNoModifiedFiles: {
        command: 'git diff --cached --exit-code --no-patch'
      }
    }
  }

  grunt.initConfig(config)

  grunt.registerTask('release-pre', 'Creates prerelease tags', releasePre)
  grunt.registerTask('release-patch', 'Creates patch-level tag & advances minor tag, and, optionally, the latest tag', releasePatch)
  grunt.registerTask('release-minor', 'Creates maintenance branch, patch- & minor-level tags, and advances latest tag', releaseMinor)
  grunt.registerTask('advanceLatestTagIfNecessary', 'Advances tag "latest" if necessary', function () {
    var done = this.async()
    var scrubTags = function (versionsString) {
      if (!('' + versionsString).trim()) return []
      return versionsString
        .replace(new RegExp('\\s*' + latestTag), ' ') // remove latestTag
        .replace(/\s+/g, ' ') // normalize whitespace
        .trim()
        .split(' ')
        .reduce(function (filteredDistincts, v) { // only keep non-prerelease valid semver versions (of the form a.b.c)
          if (semver.valid(v)) {
            v = semver.clean(v).replace(/\-.*$/, '')
            if (filteredDistincts.indexOf(v) === -1) filteredDistincts.push(v)
          }
          return filteredDistincts
        }, [])
    }
    var cmd
    async.waterfall([ function (next) {
      cmd = 'git rev-list -n 1 ' + latestTag // returns commit that latestTag points to
      grunt.log.writeln('finding commit that latest tag "' + latestTag + '" points to with:')
      grunt.log.writeln(cmd)
      exec(cmd, function (err, stdout) {
        stdout = '' + stdout.trim()
        if (err && err.code == 128) {
          grunt.log.writeln('no latest tag "' + latestTag + '" found')
          err = 'create'
        } else {
          grunt.log.writeln('latest tag "' + latestTag + '" points to commit ' + stdout)
        }
        return next(err, stdout)
      })
    }, function (stdout, next) {
      stdout = '' + stdout.trim()
      cmd = 'git tag --points-at ' + stdout // returns all tags that point to commit
      grunt.log.writeln('finding all tags that point to commit ' + stdout + ' with:')
      grunt.log.writeln(cmd)
      exec(cmd, next)
    }, function (stdout, stderr, next) {
      grunt.log.writeln('all tags that point to latest tag "' + latestTag + '":')
      grunt.log.writeln(stdout.replace(/\s+/g, ' ').trim())
      var mostRecentTagAtLatest = scrubTags(stdout).sort(semver.rcompare)[ 0 ] // get the latest version
      if (mostRecentTagAtLatest) {
        next(null, mostRecentTagAtLatest)
      } else {
        grunt.log.writeln('WARN: erroneous latest tag "' + latestTag + '": no other tags found at its commit')
        return next('skip')
      }
    }, function (mostRecentTagAtLatest, next) {
      var create = false
      if (semver.gt(patchTag, mostRecentTagAtLatest)) {
        create = true
        grunt.log.writeln('new release ' + patchTag + ' is NEWER than latest release ' + mostRecentTagAtLatest)
      } else {
        grunt.log.writeln('new release ' + patchTag + ' is OLDER than latest release ' + mostRecentTagAtLatest)
      }
      next(null, create)
    } ], function (err, create) {
      if (err && !(create = (err === 'create'))) {
        if (err === 'skip') {
          grunt.log.error('NOT advancing erroneous latest tag "' + latestTag + '"')
          return done()
        }
        grunt.log.error(err)
        return done(false)
      }
      if (create) {
        // create latestTag to point at same commit as patchTag
        grunt.log.writeln('advancing latest tag "' + latestTag + '" to same commit as ' + patchTag)
        grunt.task.run([ 'gittag:latest', 'gitpush:latestTag' ])
      } else {
        grunt.log.writeln('NOT advancing latest tag "' + latestTag + '"')
      }
      done()
    })
  })

  for (var s in config.shell) {
    if (config.shell[ s ].usage) {
      grunt.registerTask(s, config.shell[ s ].usage, 'shell:' + s)
    }
  }
  grunt.registerTask('help', 'Prints this help message.', function () {
    console.log('\n  Usage: grunt command ... # Issues grunt command(s).\n ')
    console.log('  A management script for running a grunt tasks.\n')
    console.log('  Commands:\n')
    var tasks = Object.keys(grunt.task._tasks).filter(function (name) {
      return name !== 'shell'
    })
    var usageMax = tasks.reduce(function (max, v) {
      return Math.max(max, v.length)
    }, 0)
    var pad = new Array(usageMax).join(' ')
    tasks.forEach(function (taskName) {
      var task = grunt.task._tasks[ taskName ]
      var name = (taskName + pad).substr(0, usageMax)
      console.log('    ' + name + '  ' + task.info)
    })
  })
}