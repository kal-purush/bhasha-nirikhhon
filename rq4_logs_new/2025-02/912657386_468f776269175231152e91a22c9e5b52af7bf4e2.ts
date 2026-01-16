import { Roles } from '@common/common/decorators/role.decorator';
import { RoleGuard } from '@common/common/guards/role/role.guard';
import { DatatableType } from '@common/common/types/datatable';
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
import { PermissionService } from './permission.service';
import { successResponse } from '@common/common/reponses/success.response';
import { errorResponse } from '@common/common/reponses/error.response';
import { CreatePermissionDto } from './dto/create-permission.dto';
import { UpdatePermissionDto } from './dto/update-permission.dto';

@Controller('permission')
@UseGuards(RoleGuard)
export class PermissionController {
  constructor(private readonly permissionService: PermissionService) {}

  @Get()
  @Roles(['superuser'])
  async getPermissions(
    @Query() datatable: DatatableType,
    @Res() res: Response,
  ) {
    try {
      const data = await this.permissionService.getPermissions(datatable);
      return res
        .status(200)
        .json(successResponse(200, 'Success get permissions', data));
    } catch (error) {
      return errorResponse(res, error);
    }
  }

  @Post()
  @Roles(['superuser'])
  async createPermission(
    @Body() data: CreatePermissionDto,
    @Res() res: Response,
  ) {
    try {
      await this.permissionService.createPermission(data);
      return res
        .status(201)
        .json(successResponse(201, 'Success create permission', null));
    } catch (error) {
      return errorResponse(res, error);
    }
  }

  @Get(':id')
  @Roles(['superuser'])
  async findOne(@Param('id') id: string, @Res() res: Response) {
    try {
      const data = await this.permissionService.findOne(id);
      return res
        .status(200)
        .json(successResponse(200, 'Success get permission', data));
    } catch (error) {
      return errorResponse(res, error);
    }
  }

  @Put(':id')
  @Roles(['superuser'])
  async updatePermission(
    @Param('id') id: string,
    @Body() data: UpdatePermissionDto,
    @Res() res: Response,
  ) {
    try {
      await this.permissionService.updatePermission(id, data);
      return res
        .status(200)
        .json(successResponse(200, 'Success update permission', null));
    } catch (error) {
      return errorResponse(res, error);
    }
  }

  @Delete(':id')
  @Roles(['superuser'])
  async deletePermission(@Param('id') id: string, @Res() res: Response) {
    try {
      await this.permissionService.deletePermission(id);
      return res
        .status(200)
        .json(successResponse(200, 'Success delete permission', null));
    } catch (error) {
      return errorResponse(res, error);
    }
  }
}