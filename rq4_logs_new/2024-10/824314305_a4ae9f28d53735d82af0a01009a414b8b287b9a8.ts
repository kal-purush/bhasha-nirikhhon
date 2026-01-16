import { NextRequest, NextResponse } from 'next/server'

export async function GET(_request: NextRequest) {
  const params = _request.nextUrl.searchParams
  const group_id = params.get("group_id")
  try {
    const requestUrl = `${process.env.LAMBDA_GROUP_ATTENDEES}?group_id=${group_id}`
    console.log("GET -> Conor: ",requestUrl)
    const apiResponse = await fetch(requestUrl, {
      method: 'GET',
    })
    const responseData = await apiResponse.json()
    console.log("GET <- Conor:",responseData)
    return NextResponse.json({
      data: responseData
    },{status: apiResponse.status});
  } catch (err) {
    return NextResponse.json( err.message, {status: err.statusCode || 500});
  }
}