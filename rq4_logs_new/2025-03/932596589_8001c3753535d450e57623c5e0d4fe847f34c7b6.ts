import { createDocs, getAllDocs } from "@/data/PayrolDocs.data";
import { CreatePayrollDocs } from "@/lib/dto/PayrollDocsDto.dto";
import { NextResponse } from "next/server";

export async function GET() {
    try {
        const resp = await getAllDocs();
        
        return NextResponse.json(resp,{status:200})
    } catch (error) {
        return NextResponse.json({ message: 'Something went wrong' }, { status: 500 });
    }
   
  }
  

  export async function POST(req: Request) {
    try {
      const body = (await req.json()) as CreatePayrollDocs
     
  
    
      // Parse request body
      const resp = await createDocs(body)
      return NextResponse.json({ data: resp }, { status: 200 })
    } catch (error) {
      return NextResponse.json(
        { message: "Internal Server Error" },
        { status: 500 }
      )
    }
  }
  