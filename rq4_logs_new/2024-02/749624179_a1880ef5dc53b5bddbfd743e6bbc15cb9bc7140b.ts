import { LoginIProps } from "@/types";
import { NextRequest, NextResponse } from "next/server";
import prisma from "@/lib/prisma";
import JWT from "jsonwebtoken"
import { cookies } from 'next/headers'

const secretKey = "gfjgjfghjsjdkdsjkdjkds";

export const POST = async (request: NextRequest) => {
	try {
		const body: LoginIProps = await request.json();
		const { email, password } = body;
		if (!email || !password) {
			return NextResponse.json({ message: "Invalid request. Email and password are required." });
		};
		const [branch, donor] = await Promise.all([
			prisma.branch.findUnique({ where: { email } }),
			prisma.donor.findUnique({ where: { email } })
		]);
		if (!branch) {
			if (donor) {
				if (donor.password === password) {
					const payload = { id: donor.id, status: donor.status, username: donor.username };

					const token = JWT.sign(payload, secretKey, { expiresIn: "1h" })
					cookies().set("token", token);
					return NextResponse.json({ message: "Login successfully", token });
				} else {
					return NextResponse.json({ message: "Your password is incorrect" });
				}
			} else {
				return NextResponse.json({ message: "User not registered" });
			}
		} else {
			if (branch.password === password) {
				const payload = { id: branch.id, status: branch.status, username: branch.username };
				const token = JWT.sign(payload, secretKey, { expiresIn: "1h" })
				cookies().set("token", token);
				return NextResponse.json({ message: "Login successfully", token });
			} else {
				return NextResponse.json({ message: "Your password is incorrect" });
			}
		};
		// Alternative
		// const branch = await prisma.branch.findUnique({
		// 	where: {
		// 		email
		// 	}
		// })
		// if (!branch) {
		// 	const donor = await prisma.donor.findUnique({
		// 		where: {
		// 			email
		// 		}
		// 	});
		// 	if (donor) {
		// 		if (donor.password === password) {
		// 			return NextResponse.json({ message: "login Successfully", donor });
		// 		} else {
		// 			return NextResponse.json({ message: "your password  is incorrect" });
		// 		}
		// 	} else {
		// 		return NextResponse.json({ message: "User Not register" });
		// 	}
		// } else {
		// 	if (branch.password === password) {
		// 		return NextResponse.json({ message: "Login Successfully", branch })
		// 	} else {
		// 		return NextResponse.json({ message: "your password  is incorrect" });
		// 	}
		// };
	} catch (error) {
		console.log({ error });
		throw new Error("server Error");
	}
}