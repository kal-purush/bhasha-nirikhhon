import {
  ExecutionContext,
  ForbiddenException,
  Injectable,
} from "@nestjs/common";
import { AuthGuard } from "@nestjs/passport";
import { UserDtoIds } from "./owner.guards/utills";

@Injectable()
export class CreatorGuards extends AuthGuard("jwt") {
  constructor() {
    super({});
  }

  canActivate(context: ExecutionContext) {
    const udi = UserDtoIds(context);
    if (udi.userDtoId == udi.userID) {
      return true;
    } else
      throw new ForbiddenException(
        "Forbidden operation for user. Dont try put id which is not your"
      );
  }
}