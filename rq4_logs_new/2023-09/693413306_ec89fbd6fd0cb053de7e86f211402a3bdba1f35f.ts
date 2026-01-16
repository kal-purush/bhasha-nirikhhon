import { NextResponse, NextRequest } from "next/server";
import {prisma} from '@/lib/prisma';
import {verify} from 'jsonwebtoken'
import {cookies} from 'next/headers'

export async function POST (req: NextRequest){
  const accessToken = req.headers.get('Authorization');
  if(!accessToken) return NextResponse.json({success: false, message: 'Unauthorization'});

  if(!process.env.SECRET_KEY_AC) return NextResponse.json({success: false, statusCode: 'env error'});
  const data = verify(accessToken, process.env.SECRET_KEY_AC) as {username: string}


  try{
    await prisma.users.update({where: {username: data.username}, data: {refreshToken: ''}})
    cookies().delete('Authorization')
    return NextResponse.json({success: true, message: 'Logged out'});
  }catch(err){
    return NextResponse.json({success: false, message: err});
  }
}