import { NextRequest, NextResponse } from 'next/server'
import {
  createUserByEmail,
  findUserByEmail,
} from '@/app/api/v1/register/service'

export async function POST(req: NextRequest) {
  const { email, password } = await req.json()

  const user = await findUserByEmail(email)
  if (user) {
    return NextResponse.json(
      {
        data: null,
        message: 'User already exists',
      },
      {
        status: 400,
      }
    )
  }
  const result = await createUserByEmail(email, password)
  if (result) {
    return NextResponse.json({
      data: result,
      message: 'User created',
    })
  } else {
    return NextResponse.json(
      {
        data: null,
        message: 'User not created',
      },
      {
        status: 500,
      }
    )
  }
}