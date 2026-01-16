import { Webhook } from 'svix'
import { headers } from 'next/headers'
import { WebhookEvent } from '@clerk/nextjs/server'
import { env } from '@/data/env/server'
import { UserSubscriptionTable } from '@/drizzle/schema'
import { createUserSubscription } from '@/server/db/subscription'
import { deleteUser } from '@/server/db/user'

export async function POST(req: Request) {
    const headerPayload = headers()
    const svixId = (await headerPayload).get("svix-id")
    const svixTimestamp = (await headerPayload).get("svix-timestamp")
    const svixSignature = (await headerPayload).get("svix-signature")
  
 

  // If there are no headers, error out
  if (!svixId || !svixTimestamp || !svixSignature) {
    return new Response('Error occured -- no svix headers', {
      status: 400,
    })
  }

  // Get the body
  const payload = await req.json()
  const body = JSON.stringify(payload)

  // Create a new Svix instance with your secret.
  const wh = new Webhook(env.CLERK_WEBHOOK_SECRET)

  let event: WebhookEvent

  // Verify the payload with the headers
  try {
    event = wh.verify(body, {
      'svix-id': svixId,
      'svix-timestamp': svixTimestamp,
      'svix-signature': svixSignature,
    }) as WebhookEvent
  } catch (err) {
    console.error('Error verifying webhook:', err)
    return new Response('Error occured', {
      status: 400,
    })
  }

  switch (event.type) {
    case "user.created": {
      await createUserSubscription({
        clerkUserId: event.data.id,
        tier: 'Free'
      })

      break
    }
    case "user.deleted": {
      if (event.data.id != null) {
        // const userSubscription = await getUserSubscription(event.data.id)
        // if (userSubscription?.stripeSubscriptionId != null) {
        //   await stripe.subscriptions.cancel(
        //     userSubscription?.stripeSubscriptionId
        //   )
        // }
        await deleteUser(event.data.id)
      } 
    }
  }
  

  return new Response('', { status: 200 })
}