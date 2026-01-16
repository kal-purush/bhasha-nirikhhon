
import {Request, Response} from 'express';

// Exports
	export abstract class ExpressController {

		// Attributes
			request: Request;
			response: Response;
			next: Function;

		// Constructor
			constructor (req: any, res: any, next: Function) {
				this.request = req;
				this.response = res;
				this.next = next;
			}
			public sendResponse (status: number, data: any) {
				this.response.status(status);
				this.response.json(data);
				this.response.end();
			}

			public addCookies (cookies: any) {
				let self = this;
				Object.keys(cookies).forEach(function (cookieKey) {
					self.response.cookie(cookieKey, cookies[cookieKey]);
				});
			}

	}