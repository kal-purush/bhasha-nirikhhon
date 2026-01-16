import { Body, Controller, Delete, Get, Param, Patch, Post, Query } from '@nestjs/common';
import { RoleService } from './role.service';
import { Role } from '@prisma/client';
import { CreateRoleDto } from './models/create-role.dto';
import { UpdateRoleDto } from './models/update-role.dto';

@Controller('roles')
export class RoleController {
    constructor(private readonly roleService: RoleService) { }

    @Get()
    getAll(@Query('page') page: number = 1) {
        return this.roleService.paginate(page);
    }

    @Post()
    async create(@Body() body: CreateRoleDto): Promise<Role> {
        return this.roleService.create({
            data: {
                name: body.name,
            }
        });
    }

    @Get(':id')
    async get(@Param('id') id: number) {
        return this.roleService.findOne({ where: { id: +id } })
    }

    @Patch(':id')
    async update(
        @Param('id') id: number,
        @Body() body: UpdateRoleDto
    ) {
        return this.roleService.update(+id, body);
    }

    @Delete(':id')
    async remove(@Param('id') id: number) {
        return this.roleService.remove(+id);
    }
}