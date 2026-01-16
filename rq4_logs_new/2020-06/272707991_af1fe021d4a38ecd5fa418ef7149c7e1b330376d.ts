import {  
  ExceptionFilter,
  Catch,
  ArgumentsHost,
  HttpException,
  HttpStatus, 
} from '@nestjs/common';

@Catch()
export class AllTasksExceptionsFilter implements ExceptionFilter {
  // eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
  catch(exception: unknown, host: ArgumentsHost) {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse();
    const request = ctx.getRequest();

    const status =
      exception instanceof HttpException
        ? exception.getStatus()
        : HttpStatus.INTERNAL_SERVER_ERROR;

    response.status(status).json({
      statusCode: status,
      timestamp: new Date().toISOString(),
      path: request.url,
      message: 'La base de datos dejo de funcionar'
    });
  }
}