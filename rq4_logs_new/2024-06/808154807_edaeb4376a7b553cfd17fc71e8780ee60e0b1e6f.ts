import Adapter from "src/common/adapter/adapter";
import { CreateUserDto } from "./dto/create-user.dto";
import { UpdateUserDto } from "./dto/update-user.dto";
import { User } from "./schema/user.schema";
import { Injectable } from "@nestjs/common";

@Injectable()
export default class UserAdapter implements Adapter<User, CreateUserDto, UpdateUserDto> {

    updateToEntity(dto: UpdateUserDto): User {
        return new User();
    }
    
    createToEntity(dto: CreateUserDto): User {
        return new User();
    }
    
}