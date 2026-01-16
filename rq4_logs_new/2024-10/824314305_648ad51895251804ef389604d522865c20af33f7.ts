import { createClerkClient } from '@clerk/backend';
import { auth } from '@clerk/nextjs/server';
import { NextRequest } from 'next/server';

export async function GET(_req: NextRequest) {
  const clerkClient = createClerkClient({ secretKey: process.env.CLERK_SECRET_KEY });
  const {userId} = auth();

  if(!userId){
    return Response.json({error: "User is not signed in."}, { status: 401 });
  }

  const requestingUser = await clerkClient.users.getUser(userId);
  if(!requestingUser.publicMetadata.admin){
    return Response.json({error: "User is does not have permissions."}, { status: 401 });
  }

  const summaryUrl = process.env.LAMBDA_MEAL_SUMMARY
  console.log(summaryUrl)
  const summaryResponse = await fetch(summaryUrl, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })
  const summaryData = await summaryResponse.json()

  return summaryResponse.ok ? Response.json(summaryData) : Response.json({message: "Error"}, {status: 500})

}