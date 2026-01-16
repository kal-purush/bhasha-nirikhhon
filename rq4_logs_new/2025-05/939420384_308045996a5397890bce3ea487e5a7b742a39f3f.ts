export class AppError extends Error {
	statusCode: number;
	constructor(message: string, statusCode: number) {
		super(message);
		this.statusCode = statusCode;
		this.name = this.constructor.name;
		Object.setPrototypeOf(this, new.target.prototype);
		Error.captureStackTrace(this, this.constructor);
	}
}

export class NotFoundError extends AppError {
	constructor(message = "Resource not found") {
		super(message, 404);
	}
}

export class BadRequestError extends AppError {
	constructor(message = "Bad request") {
		super(message, 400);
	}
}

export class UnauthorizedError extends AppError {
	constructor(message = "Unauthorized") {
		super(message, 401);
	}
}

export class ForbiddenError extends AppError {
	constructor(message = "Forbidden") {
		super(message, 403);
	}
}

export class InternalServerError extends AppError {
	constructor(message = "Internal server error") {
		super(message, 500);
	}
}

export class ConflictError extends AppError {
	constructor(message = "Conflict") {
		super(message, 409);
	}
}

export class UnprocessableEntityError extends AppError {
	constructor(message = "Unprocessable Entity") {
		super(message, 422);
	}
}

export class ServiceUnavailableError extends AppError {
	constructor(message = "Service Unavailable") {
		super(message, 503);
	}
}