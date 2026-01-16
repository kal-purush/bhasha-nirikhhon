const utils = {}

utils.timeUntil = function(date) {
    let pad = n => n < 10 ? `0${n}` : `${n}`
    let diff = date - (new Date())

    const SECOND = 1000
    const MINUTE = SECOND * 60
    const HOURS = MINUTE * 60
    const DAYS = HOURS * 24

    let days = diff / DAYS | 0
    let hours = diff % DAYS / HOURS | 0
    let minutes = diff % DAYS % HOURS / MINUTE | 0
    let seconds = diff % DAYS % HOURS % MINUTE / SECOND | 0

    return {days, hours, minutes, seconds}
}

utils.timeUntilString = function(date) {
    let t = utils.timeUntil(date)
    let str = `${t.seconds} seconds`

    if(t.minutes > 0) {
        str = `${t.minutes} minutes ${str}`
    }

    if(t.hours > 0) {
        str = `${t.hours} hours ${str}`
    }

    if(t.days > 0) {
        str = `${t.days} days ${str}`
    }

    return str
}

utils.worldState = function(bot) {
    const WorldState = require("warframe-worldstate-parser")
    let worldState = bot.mydb.get("isicWarframeWorldState").value()

    if(!worldState) {
        return null
    }

    let data = JSON.stringify(worldState)
    return new WorldState(data)
}

module.exports = utils