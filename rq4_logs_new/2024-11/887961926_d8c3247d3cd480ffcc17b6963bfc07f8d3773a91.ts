import { PrismaClient } from '@prisma/client';
import { NextResponse } from 'next/server';
import { verifyToken } from '@/app/utils/auth';
import { cookies } from 'next/headers';

const prisma = new PrismaClient();

export async function PUT(request: Request) {
  try {
    const cookieStore = await cookies();
    const token = cookieStore.get('auth_token');
    
    if (!token) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const payload = await verifyToken(token.value);
    if (!payload || typeof payload === 'string') {
      return NextResponse.json({ error: 'Invalid token' }, { status: 401 });
    }

    const { status, profilePic } = await request.json();

    const updatedUser = await prisma.user.update({
      where: { id: payload.userId as string },
      data: {
        status,
        profilePic,
        lastSeen: new Date(),
      },
      select: {
        id: true,
        email: true,
        firstName: true,
        lastName: true,
        profilePic: true,
        status: true,
        lastSeen: true,
        isOnline: true,
      },
    });

    return NextResponse.json(updatedUser);
  } catch (error) {
    console.error('Profile update error:', error);
    return NextResponse.json({ error: 'Error updating profile' }, { status: 500 });
  }
} 