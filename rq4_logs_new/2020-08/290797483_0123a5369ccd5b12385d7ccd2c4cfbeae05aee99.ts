import { Controller, Get } from "@nestjs/common";

@Controller()
export class AppController {
    @Get("/")
    ok(): string {
        return "OK";
    }
}