import { createRouteHandlerClient } from "@supabase/auth-helpers-nextjs"
import { cookies } from "next/headers"
import { NextResponse } from "next/server";

export async function PUT(request: Request){
    const supabase = createRouteHandlerClient({cookies});
 


    const {place} = await request.json()
    console.log("from the api route", place)
    const countdata = await supabase.from("titbit").select("Freq").eq('place',place)
    console.log("The frequency data ",countdata.data![0].Freq)
    let count = countdata.data![0].Freq + 1
    console.log("The incremented value,",count)
   const { data } = await supabase
   .from("titbit")
   .update({'Freq':count})
   .eq('place',place)
   console.log("The response afte the update ",data)
   return NextResponse.json(data);
}