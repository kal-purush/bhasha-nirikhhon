import { db } from '@/lib/db';
import { NextRequest, NextResponse } from 'next/server';
import { ApiError } from '@/types';
import { MediaRelease } from '@/types/db';

export async function GET() {
  try {
    const mediaReleases = await db.selectFrom('MediaRelease').selectAll().execute();
    const response = { data: mediaReleases };
    return NextResponse.json(response);
  } catch (error) {
    console.error('Error fetching media releases:', error);
    const apiError: ApiError = { message: 'Failed to fetch media releases' };
    return NextResponse.json(apiError, { status: 500 });
  }
}

export async function POST(request: NextRequest) {
  try {
    const body: MediaRelease = await request.json();
    const { title, description, href, imagePath } = body;
    if (!title || !description || !href || !imagePath) {
      const apiError: ApiError = { message: 'Missing requestuired fields' };
      return NextResponse.json(apiError, { status: 400 });
    }

    const newMediaRelease = await db
      .insertInto('MediaRelease')
      .values({
        title,
        description,
        href,
        imagePath,
      })
      .returningAll()
      .executeTakeFirstOrThrow();

    const response = {
      data: newMediaRelease,
      message: 'Media release created successfully',
    };
    return NextResponse.json(response, { status: 201 });
  } catch (error) {
    console.error('Error creating media release:', error);
    const apiError: ApiError = { message: 'Failed to create media release' };
    return NextResponse.json(apiError, { status: 500 });
  }
}