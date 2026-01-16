import { pickSeats } from "@/lib/seat";
import { createTRPCRouter, publicProcedure } from "@/server/api/trpc";
import { z } from "zod";

export const seatRouter = createTRPCRouter({
	seats: publicProcedure.query(async ({ ctx }) => {
		await new Promise((resolve) => setTimeout(resolve, 3000));
		return ctx.db.seat.findMany();
	}),
	selectSeats: publicProcedure
		.input(z.object({ selectedSeatsIds: z.array(z.string()).min(1) }))
		.mutation(async ({ ctx, input }) => {
			const allSeats = await ctx.db.seat.findMany();

			return pickSeats(allSeats, input.selectedSeatsIds);
		}),
});