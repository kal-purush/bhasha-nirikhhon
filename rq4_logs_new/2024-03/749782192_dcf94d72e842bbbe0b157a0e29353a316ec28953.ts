import { Body, Controller, Post, Res, UseGuards } from '@nestjs/common';
import { ApiOperation, ApiTags } from '@nestjs/swagger';
import { AdminService } from './admin.service';
import { AuthAdminGuard } from '../auth/auth.guard';
import { InactiveMembersDto } from './dto/inactive-members.dto';
import { ActiveMembersDto } from './dto/active-members.dto';
import { DeleteMembersDto } from './dto/delete-members.dto';
import { Response } from 'express';

@ApiTags('관리자 API')
@UseGuards(AuthAdminGuard)
@Controller('admin')
export class AdminController {
  constructor(private readonly adminService: AdminService) {}

  @Post('active-members')
  @ApiOperation({
    summary: '멤버 활성화',
    description: '멤버를 활성화 처리합니다.',
  })
  async activeMembers(
    @Body() activeMembersDto: ActiveMembersDto,
    @Res() res: Response,
  ) {
    await this.adminService.activeMembers(activeMembersDto);
    return res.json({
      message: '멤버 활성화 처리가 완료되었습니다.',
    });
  }

  @Post('inactive-members')
  @ApiOperation({
    summary: '멤버 비활성화',
    description: '멤버를 비활성화 처리합니다.',
  })
  async inactiveMembers(
    @Body() inactiveMembersDto: InactiveMembersDto,
    @Res() res: Response,
  ) {
    await this.adminService.inactiveMembers(inactiveMembersDto);
    return res.json({
      message: '멤버 비활성화 처리가 완료되었습니다.',
    });
  }

  @Post('delete-members')
  @ApiOperation({
    summary: '멤버 삭제',
    description: '멤버를 삭제합니다.',
  })
  async deleteMembers(
    @Body() deleteMembersDto: DeleteMembersDto,
    @Res() res: Response,
  ) {
    await this.adminService.deleteMembers(deleteMembersDto);
    return res.json({
      message: '멤버 삭제 처리가 완료되었습니다.',
    });
  }
}