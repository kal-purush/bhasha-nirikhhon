import {
  ExecutionContext,
  ForbiddenException,
  Injectable,
} from "@nestjs/common";
import { AuthGuard } from "@nestjs/passport";
import { InjectRepository } from "@nestjs/typeorm";
import { Content } from "src/content/entities/content.entity";
import { Repository } from "typeorm";
import { UserEntityIds } from "./utills";
@Injectable()
export class ContentOwnerGuard extends AuthGuard("jwt") {
  constructor(
    @InjectRepository(Content)
    private contentRepository: Repository<Content>
  ) {
    super(contentRepository);
  }

  async canActivate(context: ExecutionContext) {
    const userEntityIds = UserEntityIds(context);
    const content = await this.contentRepository.findOne(
      userEntityIds.entityID,
      {
        relations: ["author"],
      }
    );
    if ((await content.author).id == userEntityIds.userID) {
      return true;
    } else throw new ForbiddenException("Forbidden operation for user");
  }
}