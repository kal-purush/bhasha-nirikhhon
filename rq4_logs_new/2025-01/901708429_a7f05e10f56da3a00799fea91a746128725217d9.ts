import { ExceptionFilter, Catch, ArgumentsHost, HttpException, Logger, HttpStatus } from '@nestjs/common';
import { Request, Response } from 'express';
import { env } from '@/config/env.config';

@Catch()
export class AllExceptionsFilter implements ExceptionFilter {
	private readonly isProduction = env.NODE_ENV === 'production';
	private readonly logger = new Logger();

	catch(exception: unknown, host: ArgumentsHost): void {
		const ctx = host.switchToHttp();
		const response = ctx.getResponse<Response>();
		const request = ctx.getRequest<Request>();
		const statusCode = exception instanceof HttpException ? exception.getStatus() : HttpStatus.INTERNAL_SERVER_ERROR;
		const message =
			exception instanceof HttpException
				? this.getHttpErrorMessage(exception)
				: exception instanceof Error
					? exception.message
					: 'Something went wrong';
		const stack = exception instanceof Error ? exception.stack : undefined;

		if (statusCode === HttpStatus.TOO_MANY_REQUESTS) {
			response.setHeader('Retry-After', '120');
		}
		response.status(statusCode).json({
			statusCode,
			message,
			...(this.isProduction ? {} : { stack }),
			timestamp: new Date().toISOString(),
			path: request.url,
		});

		this.logger.error(`(${statusCode}) ${exception} at ${request.url}`);
	}

	private getHttpErrorMessage(exception: HttpException): string | string[] {
		const response = exception.getResponse();
		if (typeof response === 'object' && 'message' in response) {
			return response.message as string;
		}
		return exception.message;
	}
}