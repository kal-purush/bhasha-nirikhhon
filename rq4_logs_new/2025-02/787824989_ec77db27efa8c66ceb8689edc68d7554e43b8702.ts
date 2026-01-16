import { NextResponse, NextRequest } from 'next/server';
import { getServerSession } from 'next-auth/next';
import { authOptions } from '@/integrations/nextAuth/auth';
import ClubService from '@/services/Club';
import { ClubSchema } from '@/lib/validation/Club';
import { errorHandler } from '@/lib/utils/handleApiError';
import { z } from 'zod';

export async function GET() {
  // Check if the user is authenticated
  const session = await getServerSession(authOptions);
  if (!session) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });

  try {
    const clubs = await ClubService.getClubs();
    return NextResponse.json(clubs);
  } catch (error) {
    return errorHandler(error);
  }
}

export async function POST(req: NextRequest) {
  // Check if the user is authenticated
  const session = await getServerSession(authOptions);
  if (!session) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });

  try {
    // Check if the body is a valid match
    const body = await req.json();
    const clubsArray = Array.isArray(body) ? body : [body];

    // Validate entries
    const clubs = z.array(ClubSchema).parse(clubsArray);

    // Upsert matches
    const clubsCreated = await Promise.all(clubs.map(async (club) => ClubService.createClub(club)));

    // log it
    console.log('Clubs created', clubsCreated);

    // Return 200
    return NextResponse.json({ message: 'Success', data: clubsCreated }, { status: 200 });
  } catch (error) {
    return errorHandler(error);
  }
}

export async function PUT(req: NextRequest) {
  // Check if the user is authenticated with the token in the cookie
  const session = await getServerSession(authOptions);
  if (!session) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });

  try {
    const body = await req.json();
    const clubsArray = Array.isArray(body) ? body : [body];
    const clubs = z.array(ClubSchema).parse(clubsArray);

    // Upsert matches
    const clubsUpserted = await Promise.all(
      clubs.map(async (club) => {
        return await ClubService.upsert(club);
      }),
    );

    // log it
    console.log('Clubs upserted', clubsUpserted);

    // Return 200
    return NextResponse.json({ message: 'Success', data: clubsUpserted }, { status: 200 });
  } catch (error) {
    return errorHandler(error);
  }
}