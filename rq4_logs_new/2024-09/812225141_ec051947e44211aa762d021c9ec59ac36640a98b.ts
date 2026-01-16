import { getMe } from '@/services/warpcast/get-me'
import { DateTime, WeekdayNumbers } from 'luxon'
import { createHash } from 'node:crypto'

interface DirectCastBody {
  type: 'direct-cast'
  data: {
    recipientFid: number
    message: string
    idempotencyKey: string
  }
}

interface EventInfo {
  weekday: WeekdayNumbers
  hour: number
  minute: number
  eventMessage: string
  eventLink: string
}

/**
 * Scheduled event handler that checks for upcoming events and sends notifications to a queue.
 * @param env - Cloudflare Worker environment variables
 */
export async function eventsHandler(env: Env) {
  const { KV: kv, QUEUE: queue } = env

  const { user } = await getMe(env)

  const farcasterUsers: number[] =
    (await kv.get('lilnouns-farcaster-users', { type: 'json' })) ?? []

  if (farcasterUsers.length === 0) {
    return
  }

  const batch: MessageSendRequest<DirectCastBody>[] = []

  const now = DateTime.utc()

  const targetTimes: EventInfo[] = [
    {
      weekday: 2,
      hour: 16,
      minute: 15,
      eventMessage:
        'The Tuesday call is at 16:15 UTC! ' +
        "A 'semi-focused' discussion on the tasks at hand in the DAO, " +
        'with the goal of leaving each week with tangible objectives.',
      eventLink: 'https://discord.gg/pSX3yrCsHw',
    }, // Tuesday at 16:15 UTC
    {
      weekday: 4,
      hour: 21,
      minute: 0,
      eventMessage:
        'Join us for another Lil Nouns Happy Hour at 21:00 UTC! ' +
        'We will review proposals, air grievances, and probably argue about ' +
        'things that do not really matter, but hey, that is what makes it fun!',
      eventLink: 'https://discord.gg/6mVmyAUPYk',
    }, // Thursday at 21:00 UTC
  ]

  for (const target of targetTimes) {
    const nextTargetTime = getNextTargetTime(
      target.weekday,
      target.hour,
      target.minute,
    )

    const minutesDifference = nextTargetTime.diff(now, 'minutes').minutes

    if (minutesDifference <= 120 && minutesDifference >= 0) {
      const message = `${target.eventMessage}\nYou can join here: ${target.eventLink}`
      const idempotencyKey = createHash('sha256').update(message).digest('hex')

      for (const recipientFid of farcasterUsers) {
        if (recipientFid === user.fid) {
          continue
        }

        const task: MessageSendRequest<DirectCastBody> = {
          body: {
            type: 'direct-cast',
            data: {
              recipientFid,
              message,
              idempotencyKey,
            },
          },
        }

        batch.push(task)
      }
    }

    if (batch.length > 0) {
      try {
        await queue.sendBatch(batch)
        console.log('Batch enqueued successfully:', batch)
      } catch (error) {
        console.error('Error enqueuing batch:', error)
      }
    }
  }
}

/**
 * Calculates the next occurrence of a target time.
 * @param weekday - Day of the week (1-7)
 * @param hour - Hour of the day (0-23)
 * @param minute - Minute of the hour (0-59)
 * @returns The DateTime object representing the next occurrence.
 */
const getNextTargetTime = (
  weekday: WeekdayNumbers,
  hour: number,
  minute: number,
): DateTime => {
  const now = DateTime.utc()
  let targetDateTime = now.set({
    weekday,
    hour,
    minute,
    second: 0,
    millisecond: 0,
  })

  if (targetDateTime <= now) {
    targetDateTime = targetDateTime.plus({ weeks: 1 })
  }

  return targetDateTime
}