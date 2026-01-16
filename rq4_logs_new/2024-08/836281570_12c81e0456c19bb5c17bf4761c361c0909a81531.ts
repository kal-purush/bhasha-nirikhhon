import { NextRequest, NextResponse } from 'next/server';
import { connectDB, MeetingCache } from '@/lib/db'; 
import { Op } from 'sequelize';
import dotenv from "dotenv";
dotenv.config();

const { BASE_URL, NEXT_PUBLIC_BASE_URL } = process.env;
  let urlConnect: string = "";

  if (NEXT_PUBLIC_BASE_URL === "http://localhost:3000") {
    urlConnect = `${NEXT_PUBLIC_BASE_URL}/api/tasks`;
  } else {
    urlConnect = `${BASE_URL}/api/tasks`;
  }

export async function POST(req: NextRequest) {
  const dataMeeting = await req.json();
  const key = JSON.stringify(dataMeeting);
  console.log("KEY - DATAMEETING en POST-------------------->", key, dataMeeting)

  try {
    await connectDB()
    await MeetingCache.upsert({
      key: key,
      result: dataMeeting,
    });

    return NextResponse.json({ message: 'Cache stored successfully' }, { status: 201 });
  } catch (error) {
    console.error('Error storing cache:', error);
    return NextResponse.json({ error: 'Error storing cache' }, { status: 500 });
  }
}


export async function GET(req: NextRequest) {
  await connectDB()
  const { searchParams } = new URL(req.url);
  //console.log("Searchparams en GET -------------------------->",searchParams)
  const dataMeeting = searchParams.get('dataMeeting');
  //console.log("Datameeting en GET -------------------------->",dataMeeting)
  
  if (!dataMeeting) {
    return NextResponse.json({ error: 'dataMeeting query parameter is required' }, { status: 400 });
  }

  const key = JSON.stringify(JSON.parse(dataMeeting));

  try {
    console.log("")
    const cachedResult = await MeetingCache.findOne({
      where: {
        key: key,
        created_at: {
          [Op.gt]: new Date(new Date().getTime() - 60 * 60 * 1000), // Solo cache creado en la última hora
        },
      },
    });
    if (cachedResult) {
        console.log("cachedResult", cachedResult)
      return //NextResponse.json(cachedResult.result, { status: 200 });
    } else {
      return NextResponse.json({ message: 'No cache found for the provided key' }, { status: 404 });
    }
  } catch (error) {
    console.error('Error retrieving cache:', error);
    return NextResponse.json({ error: 'Error retrieving cache' }, { status: 500 });
  }
}