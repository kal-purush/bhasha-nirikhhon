import { createClerkClient } from '@clerk/backend';
import { auth } from '@clerk/nextjs/server';
import { NextRequest } from 'next/server';

export async function POST(_req: NextRequest) {
  const clerkClient = createClerkClient({ secretKey: process.env.CLERK_SECRET_KEY });
  const {userId} = auth();

  if(!userId){
    return Response.json({error: "User is not signed in."}, { status: 401 });
  }

  const requestingUser = await clerkClient.users.getUser(userId);
  if(!requestingUser.publicMetadata.admin){
    return Response.json({error: "User is does not have permissions."}, { status: 401 });
  }

  const triggerUrl = process.env.LAMBDA_MEAL_TRIGGER_REMINDER
  console.log(triggerUrl)
  const triggerResponse = await fetch(triggerUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
  })

  return triggerResponse.ok ? Response.json({message: "Email triggered"}, {status: 200}) : Response.json({message: "Email Failed"}, {status: 500})

}