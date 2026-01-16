
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
			public sendError (error: any) {
				this.response.status(500);
				this.response.json(error);
				this.response.end();
			}

			public validateData (data: any, schema: any) {
				let results = { missingFields: [], corruptedFields: [] };
				Object.keys(schema).forEach(function (key) {
					
					// Default options
						schema[key].required = (typeof schema[key].required == 'undefined') ? true : schema[key].required;
					
					// Test schema
						// Required fields
						if (!data[key] && schema[key].required) results.missingFields.push(key);
						// Corrupted fields
						if (!data[key]) return;
						if (
							// Invalid type
							( schema[key].type && (typeof data[key] != schema[key].type) ) ||
							// Doesn't match regexp
							( schema[key].regexp && !schema[key].regexp.test(data[key]) )

						) {
							results.corruptedFields.push(key);
						}

				});
				if (!results.missingFields.length && !results.corruptedFields.length) return;
				return results;
			}

			public addCookies (cookies: any) {
				let self = this;
				Object.keys(cookies).forEach(function (cookieKey) {
					self.response.cookie(cookieKey, cookies[cookieKey]);
				});
			}

	};