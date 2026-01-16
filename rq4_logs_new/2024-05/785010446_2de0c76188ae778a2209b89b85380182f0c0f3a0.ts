import { timezoneKL } from "@/lib/date";
import stripe from "@/lib/stripe";
import { getServerAuthSession } from "@/server/auth";
import { db } from "@/server/db";
import dayjs from "dayjs";
import { headers } from "next/headers";
import { type NextRequest, NextResponse } from "next/server";

import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";

dayjs.extend(timezone);
dayjs.extend(utc);

export async function POST(req: NextRequest) {
	const headersList = headers();

	const session = await getServerAuthSession();

	if (!session || !session.user) {
		return NextResponse.json({ error: "Error creating checkout session" });
	}

	const body = (await req.json()) as { seats: string[] };

	const foundSeats = await db.seat.findMany({
		where: {
			id: {
				in: body.seats,
			},
			lockedByUserId: session.user.id,
			lockedTill: {
				gt: dayjs().tz(timezoneKL).toDate(),
			},
		},
	});

	if (foundSeats.length !== body.seats.length) {
		return NextResponse.json({
			error: "Some seats are not available, please select your seat and try again",
		});
	}

	const lineItems = foundSeats.map((item) => {
		return {
			price_data: {
				currency: "myr",
				product_data: {
					name: item.label,
				},
				unit_amount: item.price * 100,
			},
			quantity: 1,
		};
	});

	try {
		const checkoutSession = await stripe.checkout.sessions.create({
			mode: "payment",
			expires_at: dayjs().tz(timezoneKL).add(20, "minutes").unix(),
			submit_type: "pay",
			line_items: lineItems,
			metadata: {
				seats: JSON.stringify(body.seats), // stringify it, later we will parse it
				user_id: session.user.id,
				for: "concert",
				entity: "concert",
			},
			success_url: `${headersList.get(
				"origin",
			)}/thank-you?session_id={CHECKOUT_SESSION_ID}`,
			cancel_url: `${headersList.get("origin")}/`,
		});

		return NextResponse.json({ sessionId: checkoutSession.id });
	} catch (err) {
		console.log(err);
		return NextResponse.json({ error: "Error creating checkout session" });
	}
}