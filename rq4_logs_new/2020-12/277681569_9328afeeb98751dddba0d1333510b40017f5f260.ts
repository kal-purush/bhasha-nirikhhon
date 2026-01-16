import { Body, Controller, Post } from '@nestjs/common';
import { CreateCbtInquiryDto } from '@truepoint/shared/dist/dto/cbtinquiry/createCbtInquiry.dto';
import { ValidationPipe } from '../../pipes/validation.pipe';
import { SlackService } from '../slack/slack.service';
import { CbtInquiryEntity } from './entities/cbtinquiry.entity';
import { CbtInquiryService } from './cbtinquiry.service';

@Controller('cbtinquiry')
export class CbtInquiryController {
  constructor(
    private readonly cbtinquiryService: CbtInquiryService,
    private readonly slackService: SlackService,
  ) {}

  @Post()
  public async createInquiry(
    @Body(new ValidationPipe()) createCbtInquiryDto: CreateCbtInquiryDto,
  ): Promise<CbtInquiryEntity> {
    // 문의 생성
    const createdCbtInquiry = await this.cbtinquiryService.create(createCbtInquiryDto);
    // 슬랙 알림
    this.slackService.message({
      title: 'CBT 신청 등록 알림',
      text: 'CBT 신청 등록되었습니다. 데이터 수집 대상으로 관리 필요합니다.',
      fields: [
        { title: '성명', value: createdCbtInquiry.name, short: true },
        { title: '플랫폼 활동명', value: createdCbtInquiry.creatorName, short: true },
        { title: '플랫폼', value: createdCbtInquiry.platform, short: true },
        { title: '연락처', value: createdCbtInquiry.phoneNum, short: true },
      ],
    });
    // 생성된 엔터티정보 반환
    return createdCbtInquiry;
  }
}