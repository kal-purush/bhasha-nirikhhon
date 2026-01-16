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
import { CheckPointMasterService } from "./check_point_master.service";
import {
  CreateCheckPointMasterDto,
  DeleteCheckPointMasterDto,
  UpdateCheckPointMasterDto,
} from "./dto/checkpointMaster.dto";
import { ResponseDto } from '@app/share_lib/common.dto';
import { AuthService } from '../auth/src';
import { Request } from 'express';

@Controller("check-point-master")
// @UseGuards(JwtAuthGuard)
@UsePipes(new ValidationPipe())
export class CheckPointMasterController {
  constructor(
    private checkpointMasterService: CheckPointMasterService,
    private authService: AuthService,
  ) {}

  @Post("create-check-point")
  async CreateCheckPoint(
    @Req() req: Request,
    @Body() createCheckPointMaster: CreateCheckPointMasterDto,
  ): Promise<ResponseDto> {
    // this.authService.ValidatePrivileges(
    //   req,
    //   'MODULE_MASTER',
    //   'MANAGE_MODULE_MASTER',
    //   'write',
    // );

    return await this.checkpointMasterService.CreateCheckPoint(
      // this.authService.GetUserFromRequest(req),
      createCheckPointMaster,
    );
  }

  @Patch("update-check-point")
  async UpdateCheckPoint(
    @Req() req: Request,
    @Body() updateCheckPointMaster: UpdateCheckPointMasterDto,
  ): Promise<ResponseDto> {
    // this.authService.ValidatePrivileges(
    //   req,
    //   'MODULE_MASTER',
    //   'MANAGE_MODULE_MASTER',
    //   'update',
    // );
    return await this.checkpointMasterService.UpdateCheckPoint(
      // this.authService.GetUserFromRequest(req),
      updateCheckPointMaster,
    );
  }

  @Get("get-check-point")
  async GetCheckPoint(
    @Req() req: Request,
    @Body() { id, customer_id },
  ): Promise<ResponseDto> {
    // this.authService.ValidatePrivileges(
    //   req,
    //   'MODULE_MASTER',
    //   'MANAGE_MODULE_MASTER',
    //   'read',
    // );
    return await this.checkpointMasterService.GetCheckPoint(
      // this.authService.GetUserFromRequest(req),
      id,
      customer_id,
    );
  }

  @Patch("delete-check-point")
  async DeleteCheckPoint(
    @Req() req: Request,
    @Body() deleteCheckPointMaster: DeleteCheckPointMasterDto,
  ): Promise<ResponseDto> {
    // this.authService.ValidatePrivileges(
    //   req,
    //   'MODULE_MASTER',
    //   'MANAGE_MODULE_MASTER',
    //   'delete',
    // );
    return await this.checkpointMasterService.DeleteCheckPoint(
      // this.authService.GetUserFromRequest(req),
      deleteCheckPointMaster,
    );
  }

  @Get("get-all-check-points")
  async GetAllCheckPoints() {
    return await this.checkpointMasterService.GetAllCheckPoints();
  }
}