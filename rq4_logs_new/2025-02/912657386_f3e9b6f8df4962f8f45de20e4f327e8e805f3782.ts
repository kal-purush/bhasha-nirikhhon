import { AuthGuard } from '@common/common/guards/auth/auth.guard';
import { errorResponse } from '@common/common/reponses/error.response';
import { successResponse } from '@common/common/reponses/success.response';
import { Controller, Get, Res, UseGuards } from '@nestjs/common';
import { PermissionModel, roleModel } from '@repository/repository';
import { Response } from 'express';

@Controller('select')
@UseGuards(AuthGuard)
export class SelectController {
  @Get('roles')
  async getRoles(
    @Res() res: Response,
  ): Promise<Response<{ id: number; name: string }[]>> {
    try {
      const data = await roleModel().select();
      return res
        .status(200)
        .json(successResponse(200, 'Roles fetched successfully', data));
    } catch (error) {
      return errorResponse(res, error);
    }
  }

  @Get('permissions')
  async getPermissions(
    @Res() res: Response,
  ): Promise<Response<{ id: number; name: string }[]>> {
    try {
      const data = await PermissionModel().select();
      return res
        .status(200)
        .json(successResponse(200, 'Permissions fetched successfully', data));
    } catch (error) {
      return errorResponse(res, error);
    }
  }
}