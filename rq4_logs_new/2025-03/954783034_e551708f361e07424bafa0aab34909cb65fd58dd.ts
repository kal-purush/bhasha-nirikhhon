import { NextResponse } from "next/server";
import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

export async function POST(req: Request) {
  try {
    const data = await req.json();

    // Save form data to the database
    const newAdmission = await prisma.user.create({
      data: {
        childName: data.childName,
        dob: data.dob,
        gender: data.gender,
        photo: data.photo,
        parentsName: data.parentsName,
        contact: data.contact,
        email: data.email,
        address: data.address,
        occupation: data.occupation,
        curriculum: data.curriculum,
        schools: data.schools,
        grade: data.grade,
      },
    });

    return NextResponse.json({ success: true, data: newAdmission }, { status: 201 });
  } catch (error) {
    console.error("Error saving admission data:", error);
    return NextResponse.json({ success: false, error: "Failed to save admission data." }, { status: 500 });
  }
}