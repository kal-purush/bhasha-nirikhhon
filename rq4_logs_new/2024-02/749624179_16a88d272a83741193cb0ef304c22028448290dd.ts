import prisma from "@/lib/prisma";
import { cookies } from "next/headers";
import { NextResponse } from "next/server";
import JWT from "jsonwebtoken"

export const dynamic = 'force-dynamic'

const secretKey = "gfjgjfghjsjdkdsjkdjkds";

export const POST = async (request: Request) => {
	try {
		const body = await request.json();
		const { email, password } = body;
		if (!email || !password) {
			return NextResponse.json({ message: "Invalid request. Email and password are required." });
		};
		const user = await prisma.admin.findUnique({ where: { email } });
		if (user) {
			if (user.password === password) {
				const payload = { id: user.id, photoUrl: user.photourl, name: user.name };

				const token = JWT.sign(payload, secretKey, { expiresIn: "1h" })
				cookies().set("token", token);
				return NextResponse.json({ message: "Login successfully", token });
			} else {
				return NextResponse.json({ message: "Your password is incorrect" });
			}
		} else {
			return NextResponse.json({ message: "User Not found" });
		}
	} catch (error) {
		console.log({ error });
		throw new Error("server Error");
	}
}