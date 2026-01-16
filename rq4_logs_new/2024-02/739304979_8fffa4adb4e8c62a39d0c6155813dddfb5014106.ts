import { HttpService } from "@nestjs/axios";
import { firstValueFrom } from "rxjs";
import { CanActivate, Injectable, ExecutionContext, ForbiddenException } from "@nestjs/common";
import { Logger } from "@nestjs/common";

@Injectable()
export class CaptchaGuard implements CanActivate {
    constructor(private readonly httpService: HttpService) {}

    async canActivate(context: ExecutionContext): Promise<boolean> {
        const RECAPTCHA_SECRET = '6LffdmspAAAAAC2ZBA_dDBr70lFTjLdNwWG3b2NI'

        const { body } = context.switchToHttp().getRequest();

        const response = this.httpService
        .post(
            `https://www.google.com/recaptcha/api/siteverify?response=${body.captchaValue}&secret=${RECAPTCHA_SECRET}`
        )
        const { data } = await firstValueFrom(response)

        if (!data.success) {
            throw new ForbiddenException();
        }

        return true;
    }

}