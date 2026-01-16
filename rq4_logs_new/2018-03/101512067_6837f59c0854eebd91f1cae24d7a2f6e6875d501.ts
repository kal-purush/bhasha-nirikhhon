import { Request as Req, Response as Res, NextFunction as Next } from 'express';
import * as Ajv from 'ajv';
import { ErrorObject } from 'ajv';

interface StatusMessage {
	message: string;
	errors?: ErrorMessage[];
}
interface ErrorMessage {
	property?: string;
	error: string;
	params: Ajv.ErrorParameters;
}

/*
 |--------------------------------------------------------------------------
 | AJV
 |--------------------------------------------------------------------------
*/

export const ajv = new Ajv({ allErrors: true });

export const validateSchema = (schema: string, msg: VALIDATION_FAILED) => {
	return (req: Req, res: Res, next: Next): Res => {
		const valid = ajv.validate(schema, req.body);
		if (!valid) {
			return res.status(422).send(status(msg, ajv.errors));
		}
		next();
	};
};

export const status = (value: string, errors?: ErrorObject[]): StatusMessage => {
	const msg: StatusMessage = { 'message': value };
	if (errors) {
		msg.errors = errors.map(error => {
			const obj: ErrorMessage = {
				error: error.message,
				params: error.params
			};
			if (error.dataPath) {
				obj.property = error.dataPath.substring(1, error.dataPath.length);
			}
			return obj;
		});
	}
	return msg;
};

export enum JSchema {
	// User
	UserLoginSchema = 'UserLoginSchema',
	UserRegistrationSchema = 'UserRegistrationSchema',
	UserUpdatePasswordSchema = 'UserUpdatePasswordSchema',
	UserAdminUpdateUser = 'UserAdminUpdateUser',

	// Content
	ContentSchema = 'ContentSchema',

	// Steam
	SteamSchema = 'SteamSchema'
}


/*
 |--------------------------------------------------------------------------
 | Status Messages
 |--------------------------------------------------------------------------
*/

export const enum VALIDATION_FAILED {
	USER_MODEL = 'User object validation failed',
	CONTENT_MODEL = 'Content object validation failed',
	STEAM_MODEL = 'Steam object validation failed',
}


export const enum ROUTE_STATUS {
	INVALID = 'The requested route does not exist. Did you forget a param?',
	UNAUTHORISED = 'Unauthorized'
}



export const enum AUTH_STATUS {
	NO_USERNAME_OR_PASSWORD = 'Missing username or password',
	NO_OR_BAD_ROLE = 'Missing or bad role',
	USERNAME_NOT_AVILIABLE = 'Username already taken',
	USER_ID_NOT_FOUND = 'The provided ID doesn\'t exist',
	ACCOUNT_CREATED = 'Account created',
	ACCOUNT_DELETED = 'Account successfully deleted',
	NO_PASSWORD_OR_NEW_PASSWORDS = 'Missing password, newPassword or confirm',
	PASSWORD_AND_CONFIRM_NOT_EQUAL = 'newPassword and confirm are not equal',
	PASSWORD_DID_NOT_MATCH = 'Password did not match the current password',
	PASSWORD_UPDATED = 'Password has been successfully updated',
}

export const enum CMS_STATUS {
	NO_ROUTES = 'No routes were found',
	CONTENT_NOT_FOUND = 'Could not retrieve content for the provided route',
	DATA_UNPROCESSABLE = 'The provided data could not be processed',
	DATA_UNABLE_TO_SAVE = 'Could not save. Internal server error',
	CONTENT_DELETED = 'Content was successfully deleted',
	SEARCH_RESULT_NONE_FOUND = 'Could not find content for the given search query',
}

export const enum STEAM_STATUS {
	NO_ROUTES = 'No routes were found',
	SERVER_ONLINE = 'Steam server is online',
	SERVER_OFFLINE = 'Steam server is offline',
	SERVER_TIMED_OUT = 'Steam server request timed out',
	SERVER_NOT_FOUND = 'Could not retrieve server data for the provided route',
	DATA_UNPROCESSABLE = 'The provided data could not be processed',
	DATA_UNABLE_TO_SAVE = 'Could not save. Internal server error',
	CONTENT_DELETED = 'Server data was successfully deleted',
	CONTENT_NOT_FOUND = 'Could not find the server content requested',
}

export const enum USERS_STATUS {
	DATA_UNPROCESSABLE = 'The provided data could not be processed',
	USER_ROLE_UPDATED = 'User\'s role has been updated successfully',
	USERNAME_NOT_AVILIABLE = 'Username already taken',
}