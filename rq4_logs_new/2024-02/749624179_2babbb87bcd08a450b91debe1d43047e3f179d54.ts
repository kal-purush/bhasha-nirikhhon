import prisma from "@/lib/prisma";
import { ParamsIProps } from "@/types";
import { NextResponse } from "next/server";

export const dynamic = 'force-dynamic'
// Single branch
export const GET = async (request: Request, { params }: ParamsIProps) => {
	try {
		const { username } = params;
		// Alternative
		const user = await prisma.branch.findUnique({
			where: { username }, select: {
				username: true, name: true, email: true, photoUrl: true, status: true
			}
		});
		if (!user) {
			const users = await prisma.donor.findUnique({
				where: { username }, select: {
					username: true, name: true, email: true, photoUrl: true, status: true
				}
			});
			return NextResponse.json(users);
		}
		return NextResponse.json(user);
	} catch (error) {
		throw new Error("Server Error");
	}
};