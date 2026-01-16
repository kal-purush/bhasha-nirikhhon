import { NextResponse } from "next/server";
import prisma from "@/lib/prisma";
import { DonorIProps } from "../../../../type";

// Donor List
export const GET = async () => {
	try {
		const result = await prisma.donor.findMany();
		return NextResponse.json({ result });
	} catch (error) {
		throw new Error("Server Error");
	}
};


// Create Donor
export const POST = async (request: Request) => {
	try {
		const body: DonorIProps = await request.json();
		const { email, password, name, username, phone, photos, about } = body;
		console.log({ body });
		const result = await prisma.donor.create({
			data: { email, password, name, username, phone, photos, about }
		})
		return NextResponse.json({ message: "successfully Donor Created", result }, { status: 200 });
	} catch (error) {
		throw new Error("Server Error");
	}
}