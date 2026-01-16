import * as jwt from "jsonwebtoken";
import * as express from "express";
import config from "config";

export function expressAuthentication(
	request: express.Request,
	securityName: string,
	scopes?: string[]
): Promise<any> {
	if (securityName === "jwt") {
		const token = request.headers["authorization"];

		return new Promise((resolve, reject) => {
			if (!token) {
				reject(new Error("No token provided"));
			}
			jwt.verify(
				token!.split(" ")[1],
				config.get("app.secret"),
				function (err: any, decoded: any) {
					if (err) {
						reject(err);
					} else {
						// Check if JWT contains all required scopes
						if (scopes && scopes.length > 0) {
							for (let scope of scopes) {
								if (!decoded.scopes.includes(scope)) {
									reject(new Error("JWT does not contain required scope."));
								}
							}
						}

						resolve(decoded);
					}
				}
			);
		});
	}
	return Promise.reject({ message: "Token not implemented" });
}