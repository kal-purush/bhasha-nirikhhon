import { DonorPaymentIProps, ParamsIdIProps } from "@/types";
import prisma from "@/lib/prisma";
import { NextResponse } from "next/server";


export const dynamic = 'force-dynamic'

export const POST = async (request: Request, { params }: ParamsIdIProps) => {
	try {
		const { id } = params;
		const body: DonorPaymentIProps = await request.json();
		const { donorUsername, photoUrl, amount, method } = body;
		const payment = await prisma.donorPayment.create({
			data: {
				donorUsername, photoUrl, amount, method
			}
		});
		const deleted = await prisma.donorRequest.delete({
			where: {
				id
			}
		});
		return NextResponse.json({ message: "payment request approved Successfully", payment });
	} catch (error) {
		return NextResponse.json({ message: error });
	}
};

export const DELETE = async (request: Request, { params }: ParamsIdIProps) => {
	try {
		const { id } = params;
		await prisma.donorRequest.delete({
			where: {
				id
			}
		});
		return NextResponse.json({ message: "Payment Request Deleted" })
	} catch (error) {
		return NextResponse.json({ error });
	}
}