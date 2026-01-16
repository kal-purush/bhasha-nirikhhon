import { Webhook } from 'svix';
import { headers } from 'next/headers';
import { WebhookEvent } from '@clerk/nextjs/server';
import {
  createUser,
  deleteUser,
  updateUser,
  UserProps,
} from '@/sanity/lib/users/users';

export async function POST(req: Request) {
  const WEBHOOK_SECRET = process.env.CLERK_WEBHOOK_SECRET;

  if (!WEBHOOK_SECRET) {
    throw new Error(
      'Please add CLERK_WEBHOOK_SECRET from Clerk Dashboard to .env or .env.local',
    );
  }

  // Get the headers
  const headerPayload = await headers();
  const svix_id = headerPayload.get('svix-id');
  const svix_timestamp = headerPayload.get('svix-timestamp');
  const svix_signature = headerPayload.get('svix-signature');

  // If there are no headers, error out
  if (!svix_id || !svix_timestamp || !svix_signature) {
    return new Response('Error occurred -- no svix headers', {
      status: 400,
    });
  }

  // Get the body
  const payload = await req.json();
  const body = JSON.stringify(payload);

  // Create a new Svix instance with your secret.
  const wh = new Webhook(WEBHOOK_SECRET);

  let evt: WebhookEvent;

  // Verify the payload with the headers
  try {
    evt = wh.verify(body, {
      'svix-id': svix_id,
      'svix-timestamp': svix_timestamp,
      'svix-signature': svix_signature,
    }) as WebhookEvent;
  } catch (err) {
    console.error('Error verifying webhook:', err);
    return new Response('Error occurred', {
      status: 400,
    });
  }

  const { type: eventType, data } = evt;

  switch (eventType) {
    case 'user.created':
    case 'user.updated':
      if (!data.id || !data.email_addresses) {
        return new Response('Error occurred -- missing data', {
          status: 400,
        });
      }

      let user: UserProps = {
        clerkId: data.id,
        email: data.email_addresses[0].email_address,
        ...(data.first_name && { firstName: data.first_name }),
        ...(data.last_name && { lastName: data.last_name }),
        ...(data.image_url && { imageUrl: data.image_url }),
      };

      if (eventType === 'user.created') {
        user = { ...user, role: ['authenticated'] };

        await createUser(user);
      } else {
        await updateUser(user);
      }
      break;

    case 'user.deleted':
      if (!data.id) {
        return new Response('Error occurred -- missing data', {
          status: 400,
        });
      }
      await deleteUser({ clerkId: data.id });
      break;
  }

  return new Response('', { status: 200 });
}