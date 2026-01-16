import { NextRequest, NextResponse } from 'next/server';
import { UserType, db } from '../../../../drizzle/db';
import { getUser } from '../../../../lib/authGuardServer';
import { TeacherChapter, User } from '../../../../drizzle/schema';
import { eq } from 'drizzle-orm';
import { authMiddlewareAdmin } from '@/lib/middleware';

export async function GET(req: NextRequest) {}

export type TeacherChapterPostDTO = {
	student_id?: number;
	chapter_id: number;
	group?: string;
	students?: number[];
	description?: string;
};
export async function POST(req: NextRequest) {
	const {
		student_id,
		chapter_id,
		group,
		students,
		description,
	}: TeacherChapterPostDTO = await req.json();
	const user = authMiddlewareAdmin() as UserType;

	try {
		if (group) {
			const students = await db.query.User.findMany({
				where: eq(User.group, group),
			});

			await Promise.all(
				students.reduce<Promise<any>[]>((acc, val) => {
					acc.push(
						db.insert(TeacherChapter).values({
							teacher_id: user.id,
							chapter_id: chapter_id,
							student_id: val.id,
							description,
						})
					);
					return acc;
				}, [])
			);

			return NextResponse.json({
				type: 'success',
				message: 'Группе дано задание',
			});
		} else if (students) {
			await Promise.all(
				students.reduce<Promise<any>[]>((acc, val) => {
					acc.push(
						db.insert(TeacherChapter).values({
							teacher_id: user.id,
							chapter_id: chapter_id,
							student_id: val,
							description,
						})
					);
					return acc;
				}, [])
			);

			return NextResponse.json({
				type: 'success',
				message: 'Студентом дано задание',
			});
		} else if (student_id) {
			await db.insert(TeacherChapter).values({
				teacher_id: user.id,
				chapter_id: chapter_id,
				student_id: student_id,
				description,
			});

			return NextResponse.json({
				type: 'success',
				message: 'Студенту дано задание',
			});
		}
	} catch (error) {
		return NextResponse.json({
			type: 'error',
			message: error,
		});
	}
}
export async function PUT(req: NextRequest) {}
export async function DELETE(req: NextRequest) {}