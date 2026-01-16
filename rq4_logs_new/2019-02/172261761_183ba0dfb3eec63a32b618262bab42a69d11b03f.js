const zlib = require('zlib')
const csv = require('csv-parser')
const request = require('request')
const chalk = require('chalk')
const neo4j = require('./neo4j-connect')

class Neo4j {

  constructor () {
    this.pageIndex = 0
    this.newestComment = 0
    this.subsCreated = Object.create(null)
    this.authorsCreated = Object.create(null)
    this.relationsCreated = Object.create(null)
  }

  injestLine (line) {
    return new Promise((resolve, reject) => {
      // update newest comment
      if (line.id_base10 > this.newestComment) this.newestComment = line.id_base10

      // start neo4j session
      let session = neo4j.driver().session()
      session
      .run(`MERGE (sub:Subreddit { id: '${line.subreddit_id}' })
      MERGE (auth:Author { name: '${line.author}' })
      MERGE (auth)-[r:POSTS_TO]->(sub)`)
      .subscribe({
        onNext: function (record) {
          // record.get('r')
        },
        onCompleted: () => {
          session.close()
          resolve(true)
        },
        onError: reject
      })
    })
  }

  injestManyLines (manyLines) {
    return new Promise((resolve, reject) => {
      // update newest comment
      // if (line.id_base10 > this.newestComment) this.newestComment = line.id_base10

      let query = []

      // subs
      // let subsReferenced = Object.create(null)
      // manyLines.forEach(line => {
      //   line.subreddit_id = line.subreddit_id.replace('t5_', '')
      //   if (typeof subsReferenced[line.subreddit_id] !== 'undefined') return false // already referenced
      //   let sub = `CREATE (\`${line.subreddit_id}\`:Subreddit { id: '${line.subreddit_id}' })`
      //   if (typeof this.subsCreated[line.subreddit_id] !== 'undefined') {
      //     sub = ``
      //   }
      //   subsReferenced[line.subreddit_id] = true
      //   this.subsCreated[line.subreddit_id] = true
      //   query.push(sub)
      // })

      // authors
      let authorsReferenced = Object.create(null)
      manyLines.forEach(line => {
        if (typeof authorsReferenced[line.author] !== 'undefined') return false // already referenced
        let auth = `CREATE (\`${line.author}\`:Author { name: '${line.author}' })`
        if (typeof this.authorsCreated[line.author] !== 'undefined') {
          auth = ``
        }
        authorsReferenced[line.author] = true
        this.authorsCreated[line.author] = true
        query.push(auth)
      })
      if (query.length === 0 || query[0] === '') {
        resolve(false)
        return false
      }
      //
      // // relations
      // manyLines.forEach(line => {
      //   let id = line.author + '-' + line.subreddit_id
      //   if (typeof this.relationsCreated[id] !== 'undefined') return false
      //   let relate = `CREATE (\`${line.author}\`)-[:POSTS_TO]->(\`${line.subreddit_id}\`)`
      //   this.relationsCreated[id] = true
      //   query.push(relate)
      // })

      // start neo4j session
      let session = neo4j.driver().session()
      session
      .run(query.join(' '))
      .subscribe({
        onNext: function (record) {
          // record.get('r')
        },
        onCompleted: () => {
          session.close()
          resolve(true)
        },
        onError: reject
      })
    })
  }

  formatLine (line) {
    // this code is run right before injestLine
    // destructure
    let {id, author, subreddit_id} = line
    // convert comment id to base 10
    let id_base10 = parseInt(id, 36)
    // convert subreddit id to base 36
    subreddit_id = subreddit_id.replace('t5_', '')
    // convert base 36 subredit id to base 10
    let subreddit_id_base10 = parseInt(subreddit_id, 36)
    // bundle data baack together
    return {id, id_base10, author, subreddit_id, subreddit_id_base10}
  }

  exit (msg) {
    // shutdown logic
    console.log(msg)
    console.log('Exiting process...')
    neo4j.driver().close()
    process.exit()
  }

  run () {
    // start recursive loop
    this.next()
  }

  next () {
    // pull pages recursivly in sequence until the status code !== 200
    this.getCommentFileByNumber(this.pageIndex)
    .catch(err => {
      console.log('error: 7864')
      console.error(err)
    })
    .then(async (lineStore) => {
      console.log(chalk.yellow("start injest..."))
      let chunkSize = 100
      let lineStoreChunked = this.splitArrayIntoChunks(lineStore, chunkSize)

      let lastCheckpoint = Date.now()
      for (var index in lineStoreChunked) {
        let lineChunk = lineStoreChunked[index]
        let response = await this.injestManyLines(lineChunk)
        lineChunk = null
        lineStoreChunked[index] = null
        let elapsedTime = Date.now() - lastCheckpoint
        console.log(chalk.cyan('' + (elapsedTime/chunkSize) + 'ms per item ') + chalk.magenta(`${index}/${lineStoreChunked.length}`))
        lastCheckpoint = Date.now()
      }
      // let chunkSize = 100
      // let lineStoreChunked = this.splitArrayIntoChunks(lineStore, chunkSize)
      //
      // let lastCheckpoint = Date.now()
      // for (var index in lineStoreChunked) {
      //   let lineChunk = lineStoreChunked[index]
      //   let response = await Promise.all(lineChunk.map(line => this.injestLine(this.formatLine(line))))
      //   lineChunk = null
      //   lineStoreChunked[index] = null
      //   let elapsedTime = Date.now() - lastCheckpoint
      //   console.log(chalk.cyan('' + (elapsedTime/chunkSize) + 'ms per item ') + chalk.magenta(`${index}/${lineStoreChunked.length}`))
      //   lastCheckpoint = Date.now()
      // }


      this.pageIndex++
      this.next()
      return true
    })
  }

  getCommentFileByNumber (num) {
    return new Promise((resolve, reject) => {
      let lineStore = []
      try {
        console.log(chalk.green('pull file ' + num))
        request(`https://storage.googleapis.com/reddit-guide-bucket/id_author_subreddit_id_2018_12_${this.padNumberWithZeros(num)}.csv`)
        .on('response', response => {
          if (response.statusCode !== 200) this.exit('exit on status code: ' + response.statusCode)
        })
        .on('error', reject)
        .pipe(zlib.createGunzip())
        .on('error', reject)
        .pipe(csv())
        .on('data', line => {
          lineStore.push(line)
        })
        .on('end', () => {
          resolve(lineStore)
        })
        .on('error', reject)
      } catch (e) {
        console.log('error: 597556')
        reject(e)
      }
    })
  }

  splitArrayIntoChunks(arr, chunkSize) {
    let returnArr = []
    let collect = []
    arr.forEach((item, index) => {
      collect.push(item)
      if (index % chunkSize === 0) {
        returnArr.push(collect)
        collect = []
      }
    })

    return returnArr
  }

  padNumberWithZeros (num) {
    let numString = '' + num
    while (numString.length < '000000000000'.length) {
        numString = '0' + numString
    }
    return numString
  }
}

let n = new Neo4j()
n.run()