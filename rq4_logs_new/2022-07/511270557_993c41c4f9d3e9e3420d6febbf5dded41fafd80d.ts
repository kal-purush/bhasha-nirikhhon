import { Body, Controller, Get, Post, Route, Security } from "tsoa";
import ITicket from "../types/ITicket";
import IGenericSuccessResponse from "../types/IGenericSuccessResponse";
import IGenericFailureResponse from "../types/IGenericFailureResponse";
import ticketService from "../services/ticket.service";

@Route("v1/api/ticket")
export class TicketController extends Controller {
	@Get()
	public async getTickets(): Promise<
		IGenericSuccessResponse | IGenericFailureResponse
	> {
		try {
			const res = await ticketService.getTickets();

			return {
				success: true,
				message: "Tickets.",
				data: res,
			};
		} catch (error: any) {
			return { success: false, message: error.message };
		}
	}

	@Post()
	public async createTicket(
		@Body() ticketData: any
	): Promise<IGenericSuccessResponse | IGenericFailureResponse> {
		this.setStatus(201);
		try {
			console.log("createTicket first", ticketData);
			const res = await ticketService.createTicket(ticketData);

			return {
				success: true,
				message: "Ticket create.",
				data: res,
			};
		} catch (error: any) {
			console.log("Error: ", error);
			return { success: false, message: error.message };
		}
	}
}