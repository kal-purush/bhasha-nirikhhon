import { CurrentUserDTO } from "./../../../common/dto/current-user.dto";
import {
    Get,
    Controller,
    Request,
    HttpException,
    HttpStatus,
    UseGuards
} from "@nestjs/common";
import { AuthGuard } from "@nestjs/passport";

@Controller("/api/users")
export class UsersController {
    @UseGuards(AuthGuard())
    @Get("/current")
    getCurrentUser(@Request() req): CurrentUserDTO {
        const user = req.user;
        if (!user) {
            throw new HttpException(
                "users/userDoesNotExist",
                HttpStatus.NOT_FOUND
            );
        }
        return {
            nickname: user.nickname,
            avatarUrl: user.avatarUrl,
            balance: user.balance,
            experience: user.experience,
            isAnon: user.isAnon,
            status: user.status
        };
    }
}