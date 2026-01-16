import { Roles } from '@common/common/decorators/role.decorator';
import { RoleGuard } from '@common/common/guards/role/role.guard';
import { DatatableType, SortDirection } from '@common/common/types/datatable';
import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Post,
  Put,
  Query,
  Res,
  UseGuards,
} from '@nestjs/common';
import { Response } from 'express';
import { RoleService } from './role.service';
import { successResponse } from '@common/common/reponses/success.response';
import { errorResponse } from '@common/common/reponses/error.response';
import { CreateRoleDto } from './dto/create-role.dto';
import { UpdateRoleDto } from './dto/update-role.dto';
import { paginationLength } from '@utils/utils/default/pagination-length';
import { defaultSort } from '@utils/utils/default/sort';

@Controller('role')
@UseGuards(RoleGuard)
export class RoleController {
  constructor(private readonly roleService: RoleService) {}

  @Get()
  @Roles(['superuser'])
  async getRoles(
    @Query('page') page: number,
    @Query('limit') limit: number,
    @Query('search') search: string,
    @Query('sort') sort: string,
    @Query('sortDirection') sortDirection: string,
    @Query('filter') filter: Record<string, string | boolean | Date> | null,
    @Res() res: Response,
  ) {
    const datatableRequest: DatatableType = {
      page: page || 1,
      limit: limit || paginationLength,
      search: search || null,
      sort: sort || defaultSort,
      sortDirection: (sortDirection === 'asc'
        ? 'asc'
        : 'desc') as SortDirection,
      filter: filter || null,
    };

    try {
      const data = await this.roleService.getRoles(datatableRequest);
      return res
        .status(200)
        .json(successResponse(200, 'Success get roles', data));
    } catch (error) {
      return errorResponse(res, error);
    }
  }

  @Post()
  @Roles(['superuser'])
  async createRole(@Body() data: CreateRoleDto, @Res() res: Response) {
    try {
      await this.roleService.createRole(data);
      return res
        .status(201)
        .json(successResponse(201, 'Success create role', null));
    } catch (error) {
      return errorResponse(res, error);
    }
  }

  @Get(':id')
  @Roles(['superuser'])
  async findOne(@Param('id') id: string, @Res() res: Response) {
    try {
      const data = await this.roleService.getRole(id);
      return res
        .status(200)
        .json(successResponse(200, 'Success get role', data));
    } catch (error) {
      return errorResponse(res, error);
    }
  }

  @Put(':id')
  @Roles(['superuser'])
  async updateRole(
    @Param('id') id: string,
    @Body() data: UpdateRoleDto,
    @Res() res: Response,
  ) {
    try {
      await this.roleService.updateRole(id, data);
      return res
        .status(200)
        .json(successResponse(200, 'Success update role', null));
    } catch (error) {
      return errorResponse(res, error);
    }
  }

  @Delete(':id')
  @Roles(['superuser'])
  async deleteRole(@Param('id') id: string, @Res() res: Response) {
    try {
      await this.roleService.deleteRole(id);
      return res
        .status(200)
        .json(successResponse(200, 'Success delete role', null));
    } catch (error) {
      return errorResponse(res, error);
    }
  }
}