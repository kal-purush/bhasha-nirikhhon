import { NextResponse } from 'next/server';
import { Webhook } from 'svix';
import { db } from '@rocket-house-productions/integration';
import { WebhookEvent } from '@clerk/nextjs/server';
import { headers } from 'next/headers';

export const config = {
  api: {
    bodyParser: false,
  },
};

const WEBHOOK_SECRET = process.env.CLERK_WEBHOOK_SECRET || ``;

async function validateRequest(request: Request) {
  const payloadString = await request.text();
  const headerPayload = headers();

  const svixHeaders = {
    'svix-id': headerPayload.get('svix-id')!,
    'svix-timestamp': headerPayload.get('svix-timestamp')!,
    'svix-signature': headerPayload.get('svix-signature')!,
  };
  const wh = new Webhook(WEBHOOK_SECRET);
  return wh.verify(payloadString, svixHeaders) as WebhookEvent;
}

export async function POST(req: Request, res: Response) {
  console.log('Webhook received');
  const headersList = headers();

  if (!WEBHOOK_SECRET) {
    return NextResponse.json({ message: 'Webhook secret not set' }, { status: 400 });
  }

  const svix_id = headersList.get('svix-id');
  const svix_timestamp = headersList.get('svix-timestamp');
  const svix_signature = headersList.get('svix-signature');

  if (!svix_id || !svix_timestamp || !svix_signature) {
    return NextResponse.json({ message: 'Missing headers' }, { status: 400 });
  }

  try {
    const payload = await validateRequest(req);

    const { id } = payload.data;
    const eventType = payload.type;

    switch (eventType) {
      case 'user.created': {
        console.log('User created', id);

        if (!id) {
          throw new Error('Invalid user ID');
        }

        const count = await db.account.count({
          where: {
            userId: id,
          },
        });

        if (count === 0) {
          await db.account.create({
            data: {
              userId: id,
              stripeCustomerId: '', // Add any required fields here with default or empty values
              firstName: '',
              lastName: '',
              email: '',
            },
          });
        }
        return NextResponse.json({ message: 'Success' });
      }
      case 'user.deleted': {
        console.log('User deleted', id);

        if (!id) {
          throw new Error('Invalid user ID');
        }

        await db.account.delete({
          where: {
            userId: id,
          },
        });

        return NextResponse.json({ message: 'Success' });
      }
      default: {
        console.log('Unhandled event', eventType);
        break;
      }
    }

    console.log('payload', payload);
    return NextResponse.json({ message: 'Success' });
  } catch (error) {
    console.log('error', error);
    return NextResponse.json({ message: 'Error' }, { status: 400 });
  }
}