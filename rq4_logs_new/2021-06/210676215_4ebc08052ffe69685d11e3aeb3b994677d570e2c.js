'use strict'

const cooldown = ({ bot }) => {
  const close = code => () => {
    bot.telegram.close().then(() => {
      process.exit(code)
    })
  }

  process.on('SIGHUP', close(128 + 1))
  process.on('SIGINT', close(128 + 2))
  process.on('SIGTERM', close(128 + 15))
}

module.exports = cooldown