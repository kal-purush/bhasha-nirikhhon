import { requestToRescheduleService } from "@/data/Meeting.data"
import { RequestToReschedulePayload } from "@/lib/dto/Client.dto"
import { NextResponse } from "next/server"

export async function PUT(
  req: Request,
  { params }: { params: { id: string } }
) {
  try {
    const requestBody = (await req.json()) as RequestToReschedulePayload

    const { id } = params

    const resp = await requestToRescheduleService({ id, ...requestBody })

    return NextResponse.json(
      {
        message: "Meeting rescheduled successfully",
        data: resp,
      },
      { status: 200 }
    )
  } catch (error) {
    console.error("Error in route handler:", error)

    // Return a 500 error if something goes wrong
    return NextResponse.json(
      { message: "Failed to reschedule meeting" },
      { status: 500 }
    )
  }
}