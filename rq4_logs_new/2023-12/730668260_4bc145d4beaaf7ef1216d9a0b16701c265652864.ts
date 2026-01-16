import { Body, Controller, Delete, Get, Param, Query } from "@nestjs/common";
import { UserService } from "./user.service";
import { query } from "express";
import { UpdateUserDto, UserQuery } from "./user.dto";
import { ApiBearerAuth } from "@nestjs/swagger";
import { Auth } from "src/decorators/role.decorator";

@Controller('admin/post')
@ApiBearerAuth('Authorization')
@Auth()
export class AdminUserController{
    constructor(private readonly userService: UserService){

    }

    @Get('list')
    async getListUser(@Query() query: UserQuery){
        return await this.userService.findAll(query);
    }

    @Get(':id')
    async findUser(@Param('id') id: string, @Body() body: UpdateUserDto){
        return await this.userService.updateUser(id, body);
    }
    @Delete(':id')
    async deleteUser(@Param('id') id: string){
        return await this.userService.updateUser(id,{is_deleted: true});
    }
}