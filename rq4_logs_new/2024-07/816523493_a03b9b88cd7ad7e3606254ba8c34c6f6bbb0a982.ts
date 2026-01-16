import { PrismaClient } from '@prisma/client';
import { NextResponse } from 'next/server';

const prisma = new PrismaClient();

export async function main() {
  try {
    await prisma.$connect();
  } catch (error) {
    return Error('Failed to connect to database');
  }
}

export const GET = async (req: Request, res: Response) => {
  try {
    await main();
    const users = await prisma.user.findMany();
    return NextResponse.json(users, { status: 200 });
  } catch (err) {
    return NextResponse.json({ message: 'Server error', err }, { status: 500 });
  } finally {
    await prisma.$disconnect();
  }
};