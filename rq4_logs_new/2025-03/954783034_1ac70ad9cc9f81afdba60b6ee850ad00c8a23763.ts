// import { NextResponse } from "next/server";
// import { prisma } from "@/lib/prisma";

// export async function POST(req: Request) {
//   try {
//     const data = await req.json();
//     console.log("📨 Received Data:", data); // ✅ Debug incoming data

//     // ✅ Ensure undefined fields are properly handled
//     const newAdmission = await prisma.user.create({
//         data: {
//           childName: data.childName,
//           dob: data.dob,
//           gender: data.gender,
//           photo: data.photo,
//           fatherName: data.parentsName?.[0] || null,  // Ensure field exists
//           motherName: data.parentsName?.[1] || null,
//           fatherOccupation: data.occupation?.[0] || null,
//           motherOccupation: data.occupation?.[1] || null,
//           contact1: data.contact?.[0] || "",
//           contact2: data.contact?.[1] || "",
//           email: data.email,
//           address: data.address,
//           curriculum: data.curriculum,
//           school1: data.schools?.[0] || null,
//           school2: data.schools?.[1] || null,
//           grade: data.grade,
//         },
//       });
      

//     console.log("✅ Admission Saved:", newAdmission); // ✅ Log success
//     return NextResponse.json({ success: true, data: newAdmission }, { status: 201 });
//   } catch (error) {
//     console.error("❌ Prisma Error:", error); // ✅ Log Prisma errors
//     return NextResponse.json({ success: false, error: error.message || "Failed to save admission data." }, { status: 500 });
//   }
// }