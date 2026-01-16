import { env } from "node:process";
import { CanActivate, ExecutionContext, Inject, Injectable } from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import { auth } from "express-oauth2-jwt-bearer";
import { AuthStorageService } from "@/auth/auth.storage.service";
import { AUTH_STORAGE, IS_PUBLIC_KEY } from "@/constants";

const { AUTH0_DOMAIN, AUTH0_AUDIENCE } = env;
const checkJWT = auth({
	audience: AUTH0_AUDIENCE,
	issuerBaseURL: AUTH0_DOMAIN,
});

@Injectable()
export class AuthGuard implements CanActivate {
	constructor(@Inject(AUTH_STORAGE) private storage: AuthStorageService, private reflector: Reflector) {
	}

	async canActivate(context: ExecutionContext) {
		const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC_KEY, [
			context.getHandler(),
			context.getClass(),
		]);
		if (isPublic) {
			return true;
		}
		const http = context.switchToHttp();
		const req = http.getRequest();
		await checkJWT(req, http.getResponse(), (err: unknown) => {
			if (err) {
				throw err;
			}
		});
		this.storage.setUser(req.auth.payload);
		return true;
	}
}