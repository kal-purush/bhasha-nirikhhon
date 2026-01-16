import {
  Body, Controller, Get, Param, Post, ValidationPipe,
} from '@nestjs/common';
import { CreateCbtUserDto } from '@truepoint/shared/dist/dto/cbt/createCbtUser.dto';
import { CbtInquiryEntity } from '../cbtinquiry/entities/cbtinquiry.entity';
import { CbtService } from './cbt.service';

@Controller('cbt')
export class CbtController {
  constructor(
    private readonly cbtService: CbtService,
  ) {}

  /**
   * CBT 목록 조회
   */
  @Get('list')
  private async findAll(): Promise<CbtInquiryEntity[]> {
    return this.cbtService.findList();
  }

  /**
   * 특정 CBT 신청을 조회
   * @param id 조회할 특정 CBT 신청서 Id
   */
  @Get('list/:id')
  private async findOne(
    @Param('id') id: string,
  ): Promise<CbtInquiryEntity> {
    return this.cbtService.findOne(id);
  }

  /**
   * CBT 유저 생성
   * @param createCbtUserDto CBT 유저 생성 DTO shared/dto/cbt/createCbtUser.dto.ts 확인
   */
  @Post('user')
  private async createUser(
    @Body(ValidationPipe) createCbtUserDto: CreateCbtUserDto,
  ) {
    return this.cbtService.createCbtUser(createCbtUserDto);
  }
}