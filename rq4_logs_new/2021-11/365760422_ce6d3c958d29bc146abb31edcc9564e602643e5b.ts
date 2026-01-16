import { Injectable, NestInterceptor, ExecutionContext, CallHandler } from "@nestjs/common";
import { Observable } from "rxjs";
import { tap } from "rxjs/operators";

@Injectable()
export class LoggingInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const requestIncomingTime = Date.now();

    return next.handle().pipe(
      tap(() => {
        const durationTime = Date.now() - requestIncomingTime;

        const request = context.switchToHttp().getRequest();
        const response = context.switchToHttp().getResponse();

        const httpStatusCode = response.statusCode;
        const clientIP = request.ip;
        const httpMethod = request.method;
        const url = context.switchToHttp().getRequest().url;

        const logFormat = `API service: ${new Date(
          requestIncomingTime
        ).toUTCString()} |   ${httpStatusCode}   |   ${durationTime}ms      |   ${clientIP}     |   ${httpMethod}   ${url}`;

        console.log(logFormat);
      })
    );
  }
}