import { NextResponse } from "next/server"
import { connectToDatabase } from "@/utils/mongodb"

export async function POST(req: Request) {
  try {
    const body = await req.json()
    const { name, email, college, year, phone } = body

    // Connect to MongoDB
    const { db } = await connectToDatabase()

    // Insert the registration data
    await db.collection("registrations").insertOne({
      name,
      email,
      college,
      year,
      phone,
      createdAt: new Date(),
    })

    return NextResponse.json({ message: "Registration successful" }, { status: 201 })
  } catch (error) {
    console.error("Registration error:", error)
    return NextResponse.json(
      { message: "Error creating registration" },
      { status: 500 }
    )
  }
}