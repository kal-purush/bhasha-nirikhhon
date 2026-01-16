const Promise = require('bluebird');
const path = require('path');
const chalk = require('chalk');
const dateformat = require('dateformat');
const util = require('util');

const low = require('lowdb');
const storage = require('lowdb/file-async');
const db = low(path.join(__dirname, 'requests.json'), { storage });
const requests = db('requests');

const stats = (set) => set.reduce((stats, request, index, requests) => {
    const duration = request.duration;
    stats.total = requests.length;
    stats.avg = stats.avg + (duration / stats.total);
    stats.min = Math.min(stats.min, duration);
    stats.max = Math.max(stats.max, duration);
    return stats;
}, {
    total : 0,
    avg : 0,
    min : 9999999,
    max : 0
});

// const application = stats(requests.chain().map(r => r.response).filter({ url : 'https://stemn.com/api/v1/health/application' }).value());
// const database = stats(requests.chain().map(r => r.response).filter({ url : 'https://stemn.com/api/v1/health/database' }).value());
// const models = stats(requests.chain().map(r => r.response).filter({ url : 'https://stemn.com/api/v1/health/models' }).value());

const long =
requests
.chain()
.map(r => r.response)
.filter(r => r.duration < 10000)
.filter(r => r.duration > 2000)
.sortBy('url')
.map((request) => {
    const endpoint = request.url.split('/').pop();
    return [endpoint, request.body.timings];
})
.value();

// console.log('application', application)
// console.log('database', database)
// console.log('models', models)
console.log(util.inspect(long, { depth : 3 }))