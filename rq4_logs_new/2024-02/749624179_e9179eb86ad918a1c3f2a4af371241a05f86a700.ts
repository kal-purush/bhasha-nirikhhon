import { NextResponse } from "next/server";

export const dynamic = 'force-dynamic'

export const POST = async (request: Request) => {
	try {
		const body = await request.json();
		const res = await fetch('https://arafatfoundation.vercel.app/api/branch', {
			method: "POST",
			headers: {
				"Content-type": "application/json"
			},
			body: JSON.stringify(body),
		});
		const result = await res.json();
		return NextResponse.json(result);
	} catch (error) {
		throw new Error("error");
	}
};

// export const DELETE = async (request: Request, { params }: ParamsIProps) => {
// 	try {
// 		const { username } = params;
// 		const res = await fetch(`https://arafatfoundation.vercel.app/api/branch/${username}`, {
// 			method: "DELETE"
// 		});
// 		return NextResponse.json("ok deleted");
// 	} catch (error) {
// 		throw new Error("error");
// 	}
// };