import * as dotenv from 'dotenv'
dotenv.config()

//@ts-ignore
import hg from '@hgraph.io/sdk'
import {queries, subscriptions} from './graphql'

async function main() {
  const queryResult = await hg(queries.TransactionsLast24Hrs, {
    headers: {
      'x-api-key': process.env.HGRAPH_API_KEY,
    },
  })

  console.dir(queryResult, {depth: null})

  /*
   * Make a subscription call
   */
  const unsubscribe = await hg(subscriptions.LatestTransaction, {
    // The client supports filtering the response date using jmespath - https://jmespath.org/
    filter: 'data.transaction[0].consensus_timestamp',
    next: (data: bigint) => {
      const diff = (BigInt(new Date().getTime()) - data / 1000000n) / 1000n
      console.log(`consensus_timestamp was about ${diff} seconds ago`)
    },
    error: (e: string) => {
      console.error(e)
    },
    complete: () => {
      console.log('Optionally do some cleanup')
    },
    headers: {
      'x-api-key': process.env.HGRAPH_API_KEY,
    },
  })

  // clear subscription
  setTimeout(unsubscribe, 3000)
}

main()