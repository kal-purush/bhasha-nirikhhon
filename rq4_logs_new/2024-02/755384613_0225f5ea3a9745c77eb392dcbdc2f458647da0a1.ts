import { collection, getDocs, addDoc } from "firebase/firestore";
import { NextResponse } from "next/server";
import { database } from "@/firebase/config";
import dayjs from "dayjs";
import id from "dayjs/locale/id";

export async function GET(req: Request) {
  try {
    const dataList = await getDocs(collection(database, "ratings"));
    return NextResponse.json({
      message: "Data successfully fetched👍",
      data: dataList.docs.map((doc) => ({
        ...doc.data(),
        id: doc.id,
      })),
    });
  } catch (error) {
    return NextResponse.error();
  }
}

export async function POST(req: Request) {
  try {

    const payload = await req.json();
    dayjs.locale(id);

    const data = await addDoc(collection(database, "ratings"), {
        ...payload,
        created_at: dayjs().toISOString(),
    });

    return NextResponse.json({
      message: "Rating added to completed tasks successfully👍",
      data: {
        id: data.id,
        ...payload,
      }, // Return the unique key generated for the new task
    });
  } catch (error) {
    return NextResponse.error();
  }
}