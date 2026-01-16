import * as t from "../types";

export default async function middleware(input: t.IInput, reply: t.IReply, next: () => Promise<void>) {
	if (input.connect) {
		reply.code = 554;
		reply.text = "No SMTP service here";
	}
	else if (input.command) {
		if (input.command.name === "quit") {
			reply.code = 221;
			reply.text = "Bye";
		}
		else {
			reply.code = 503;
			reply.text = "Bad sequence of commands";
		}
	}
	else {
		// must not get here
		throw new Error("Something really wrong.");
	}
}