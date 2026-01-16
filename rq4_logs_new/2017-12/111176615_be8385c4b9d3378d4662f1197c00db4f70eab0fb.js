const axios = require('axios')
const follow = require('follow')
const {red, orange, yellow, blue, emoji} = require('@buzuli/color')

const throttle = require('../lib/throttle')

module.exports = {
  command: 'couch-offset <leader-url>',
  desc: 'track a CouchDB and the offset of its follower(s)',
  builder,
  handler
}

function builder (yargs) {
  return yargs
    .option('follower-url', {
      type: 'array',
      desc: 'url of follower (may supply multiple time)',
      aliases: ['f']
    })
}

function latestSeq (url) {
  return axios
    .get(url)
    .then(({data}) => data.update_seq)
}

// Track the latest sequence for a URL
function trackSeq (url, handler) {
  const notify = throttle({minDelay: 1000, maxDelay: null})
  const reportError = (error) => {
    notify(() => {
      console.error(error)
      console.error(
        red(`Error tracking offset from leader ${blue(url)}.`),
        emoji.inject('Details above :point_up:')
      )
    })
  }

  // Get the initial sequence
  latestSeq(url)
  .then(seq => {
    handler(seq)

    // Now follow all sequence changes
    follow({db: url, since: 'now'}, (error, change) => {
      if (error) {
        reportError(error)
      } else {
        handler(change.seq)
      }
    })
  })
  .catch(error => reportError(error))
}

let leaderSeq = 0
function handler (argv) {
  const {leaderUrl, followerUrl: followers} = argv
  console.log(`leader: ${blue(leaderUrl)}`)
  console.log(`followers: ${orange(followers)}`)

  const notify = throttle({
    reportFunc: () => {
      console.log(`[${blue(new Date().toISOString())}] Leader sequence is ${yellow(leaderSeq)}`)
    },
    minDelay: 250,
    maxDelay: 15000
  })

  trackSeq(leaderUrl, seq => {
    leaderSeq = seq
    notify()
  })
}