import { BadRequestException, INestApplication, ValidationPipe } from "@nestjs/common";
import { useContainer } from "class-validator";
import cookieParser from "cookie-parser";
import { AppModule } from "src/app.module";
import { HttpExceptionFilterTest } from "src/infrastructure/exception-filters/exp-filtr-test";

export const appUse = (app: INestApplication) => {
    app.use(cookieParser());
    useContainer(app.select(AppModule), { fallbackOnErrors: true });
    app.useGlobalPipes(new ValidationPipe({
        transform: true,
        stopAtFirstError: true,
        forbidUnknownValues: false,
        exceptionFactory: (errors) => {
            const errorsForResponse:  { message: string, field: string }[]  = [] ;
            errors.forEach((e: any) => {
            if (e.constraints) {
                const constraintsKey = Object.keys(e.constraints);
                constraintsKey.forEach((ckey) => {
            
                errorsForResponse.push({ message: e.constraints[ckey], field: e.property });
            });
            }
        });
        throw new BadRequestException(errorsForResponse);
        }
    }));
    app.useGlobalFilters(new HttpExceptionFilterTest());
}