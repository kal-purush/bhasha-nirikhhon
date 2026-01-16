import { Inject, Injectable, NestMiddleware, Request } from "@nestjs/common";
import { NextFunction, Response } from "express";
import { AuthStorageService } from "@/auth/auth.storage.service";
import { AUTH_STORAGE } from "@/constants";

@Injectable()
export class AuthStorageMiddleware implements NestMiddleware {
	constructor(@Inject(AUTH_STORAGE) private readonly authStorageService: AuthStorageService) {}

	use(req: Request, res: Response, next: NextFunction) {
		this.authStorageService.storage.run({
			user: req.auth.payload,
		}, next);
	}
}