import { getParser } from '../utils/csv.js'
import { logger } from '../utils/logger.js'
import { multiaddrInfo, type MultiaddrInfo } from '../utils/multiaddr-info.js'
import type { GlobalArgs, JsonRecord } from './types.js'
import type { BuilderCallback } from 'yargs'

const log = logger.forComponent('cmd-json')

export interface JsonArgs extends GlobalArgs {
  limit?: number
  start: number
}

export const description = 'print JSON representation of records from the CSV file'

export const builder: BuilderCallback<GlobalArgs, JsonArgs> = (yargv) => {
  return yargv.option('limit', {
    describe: 'Limit the number of records to convert to JSON',
    type: 'number'
  })
    .option('start', {
      default: 0,
      describe: 'Start collecting from this record (inclusive)',
      type: 'number'
    })
}

export const handler = async (argv: JsonArgs): Promise<void> => {
  const json = await getJson(argv)
  process.stdout.write(JSON.stringify(json, null, 2))
}

/**
 * TODO: Allow for newline delimited JSON so we're not loading everything into memory (i.e. group in a latter step)
 */
async function getJson (argv: JsonArgs): Promise<JsonRecord[]> {
  log('Converting records from %s to JSON', argv.csv)
  const parser = getParser(argv.csv, { columns: true })

  if (argv.start > 0) {
    log(`Starting print at record ${argv.start}`)
  }

  const recordsByPeerId = new Map<string, JsonRecord>()

  let count = 0
  for await (const record of parser) {
    if (count < argv.start) {
      count++
      continue
    }
    let maddrInfo: MultiaddrInfo

    try {
      maddrInfo = multiaddrInfo(record.maddr)

      count++
    } catch (err) {
      log.error('Error processing record %d: %O', count, record, err)
      maddrInfo = { invalidMultiaddr: true } // we shouldn't have to set this, nor catch the error but TODO fix.
    }

    const existingRecord = recordsByPeerId.get(record.peer_id)
    if (existingRecord != null) {
      existingRecord.maddr.push(record.maddr)
      // Merge maddrInfo of existing record with new maddrInfo
      for (const [key, value] of Object.entries(maddrInfo)) {
        // If value is true, overwrite existingRecord's value
        if (value === true) {
          // @ts-expect-error - object.entries removes the keytype. TODO fix.
          existingRecord.maddrInfo[key] = true
        }
      }
    } else {
      // If no record exists for this peerId, create a new one
      recordsByPeerId.set(record.peer_id, {
        ...record,
        maddr: [record.maddr],
        maddrInfo
      })
    }

    if (argv.limit != null) {
      if (count - argv.start >= argv.limit) {
        log.trace('Reached limit of %d record(s)', argv.limit)
        break
      }
    }
  }
  log('No more records to add')

  return Array.from(recordsByPeerId.values())
}