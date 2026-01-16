import bcrypt from 'bcrypt'
import prisma from '@/lib/prisma'
import { NextResponse } from 'next/server'
import { prismaConnect } from '@/utils/prismaConnect'

export async function POST(request: Request) {
  try {
    await prismaConnect()
    const data = await request.json()

    const user = await prisma.user.findUnique({
      where: {
        email: data.email
      }
    })

    const newPassword = await bcrypt.hash(data.password, 10)

    const updatedUser = await prisma.user.update({
      where: {
        id: user.id
      },
      data: {
        hashedPassword: newPassword
      }
    })

    return NextResponse.json(updatedUser, { status: 200 })
  } catch (error) {
    console.log('[CHANGE PASSWORD]', error)
    return NextResponse.json({ message: error.message }, { status: 500 })
  }
}