import { Permissions } from '@common/common/decorators/permission.decorator';
import { PermissionGuard } from '@common/common/guards/permission/permission.guard';
import { Controller, Get, Res, UseGuards } from '@nestjs/common';
import { Response } from 'express';

@Controller('test-permission')
@UseGuards(PermissionGuard)
export class TestPermissionController {
  @Get('/')
  @Permissions(['view:admin-dashboardasd'])
  getIndex(@Res() res: Response) {
    return res.status(200).json({ message: 'Permission Test Successful!' });
  }
}