import dbConnect from "../../../../lib/connectDb";
import { authOptions } from "../../(auth)/auth/[...nextauth]/options";
import mongoose from "mongoose";
import { NextResponse } from "next/server";
import { getServerSession, User } from "next-auth";
import { StudentModel, AttendanceModel, TeacherModel, Student } from "../../../../model/User";

export async function PATCH(request: Request) {
    await dbConnect();
    const session = await getServerSession(authOptions);
    const user: User = session?.user as User;

    if (!session || !user) {
        return NextResponse.json({ error: 'Unauthorized. User must be logged in.' }, { status: 401 });
    }

    const userId = new mongoose.Types.ObjectId(user._id);

    const student: Student | null = await StudentModel.findOne({ user_id: userId });

    if (!student) {
        return new Response(
            JSON.stringify({ success: false, message: 'Student not found' }),
            { status: 400, headers: { 'Content-Type': 'application/json' } }
        );
    }

    const requestBody = await request.json();
    const urlParts = request.url.split('/');
    const teacherId = urlParts[urlParts.length - 2];
    const subjectId = urlParts[urlParts.length - 1];

    if (!teacherId) {
        return new Response(
            JSON.stringify({ success: false, message: 'Teacher ID is null' }),
            { status: 400, headers: { 'Content-Type': 'application/json' } }
        );
    }

    if (!requestBody) {
        return new Response(
            JSON.stringify({ success: false, message: 'Request body is null' }),
            { status: 400, headers: { 'Content-Type': 'application/json' } }
        );
    }

    const attendanceMap = requestBody.attendanceMap;

    const attendance = await AttendanceModel.findOne({ 
        subjectId: new RegExp(`^${subjectId}$`, 'i'),
        teacherId: teacherId
    });

    if (!attendance) {
        return new Response(
            JSON.stringify({ success: false, message: 'Attendance not found' }),
            { status: 400, headers: { 'Content-Type': 'application/json' } }
        );
    }

    const { Date: date, studentsPresent } = attendanceMap;

    const existingDateEntry = attendance.dateStudentMap.find(entry => entry.date.toISOString() === new Date(date).toISOString());

    console.log('studentsPresent:', studentsPresent);
    if (existingDateEntry) {
        existingDateEntry.studentPresent = studentsPresent;
    } else {
        attendance.dateStudentMap.push({
            date: new Date(date),
            studentPresent: studentsPresent
        });
    }
    attendance.totalClasses += 1;

    await attendance.save();

    return new Response(
        JSON.stringify({ success: true, message: 'Attendance updated successfully' }),
        { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
}