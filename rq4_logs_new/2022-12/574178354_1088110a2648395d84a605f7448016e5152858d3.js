const development = {
    name: 'development',
    db: 'Polling_API'
}

const production = {
    name: 'production',
    db: process.env.POLLING_API_DB
}

module.exports = production;