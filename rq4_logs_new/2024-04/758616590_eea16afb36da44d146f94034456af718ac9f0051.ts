import {
  Body,
  Controller,
  Delete,
  Get,
  Patch,
  Post,
  Req,
  UseGuards,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwtAuth.guard';
import { ControlMasterService } from './control_master.service';
import {
  CreateControlMasterDto,
  DeleteControlMasterDto,
  UpdateControlMasterDto,
} from './dto/controlMaster.dto';
import { ResponseDto } from '@app/share_lib/common.dto';
import { AuthService } from '../auth/src';
import { Request } from 'express';

@Controller('control-master')
// @UseGuards(JwtAuthGuard)
@UsePipes(new ValidationPipe())
export class ControlMasterController {
  constructor(
    private controlMasterService: ControlMasterService,
    private authService: AuthService,
  ) {}

  @Post('create-control')
  async CreateControl(
    @Req() req: Request,
    @Body() createControlDto: CreateControlMasterDto,
  ): Promise<ResponseDto> {
    // this.authService.ValidatePrivileges(
    //   req,
    //   'Control_MASTER',
    //   'MANAGE_Control_MASTER',
    //   'write',
    // );

    return await this.controlMasterService.CreateControl(
      // this.authService.GetUserFromRequest(req),
      createControlDto,
    );
  }

  @Patch('update-control')
  async UpdateControl(
    @Req() req: Request,
    @Body() updateControlDto: UpdateControlMasterDto,
  ): Promise<ResponseDto> {
    // this.authService.ValidatePrivileges(
    //   req,
    //   'Control_MASTER',
    //   'MANAGE_Control_MASTER',
    //   'update',
    // );
    return await this.controlMasterService.UpdateControl(
      // this.authService.GetUserFromRequest(req),
      updateControlDto,
    );
  }

  @Get('get-control')
  async GetControl(
    @Req() req: Request,
    @Body() { id, customer_id },
  ): Promise<ResponseDto> {
    // this.authService.ValidatePrivileges(
    //   req,
    //   'Control_MASTER',
    //   'MANAGE_Control_MASTER',
    //   'read',
    // );
    return await this.controlMasterService.GetControl(
      // this.authService.GetUserFromRequest(req),
      id,
      customer_id,
    );
  }

  @Patch('delete-control')
  async DeleteControl(
    @Req() req: Request,
    @Body() deleteControlDto: DeleteControlMasterDto,
  ): Promise<ResponseDto> {
    // this.authService.ValidatePrivileges(
    //   req,
    //   'Control_MASTER',
    //   'MANAGE_Control_MASTER',
    //   'delete',
    // );
    return await this.controlMasterService.DeleteControl(
      // this.authService.GetUserFromRequest(req),
      deleteControlDto,
    );
  }

  @Get('get-all-controls')
  async GetAllUsers() {
    return await this.controlMasterService.GetAllControls();
  }
}