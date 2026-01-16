import { NextResponse } from 'next/server'
console.clear()

const URL = {
  leader: 'http://210.210.210.31:30706/',
  citizen: 'http://210.210.210.31:30708/',
}

export const POST = async (req: Request) => {
  try {
    const { endpoint, body, token, guid, userType } = await req.json()

    const res = await fetch(URL[userType as 'leader' | 'citizen'] + endpoint, {
      method: 'POST',
      body: JSON.stringify(body),
      headers: {
        'Content-Type': 'application/json',
        Authorization: token,
        guid,
      },
    })

    if (res.status === 401) throw new Error('You are not authorized')

    const data = await res.json()

    return NextResponse.json(data, { status: 200 })
  } catch (err: any) {
    return NextResponse.json({ message: err.message }, { status: 401 })
  }
}