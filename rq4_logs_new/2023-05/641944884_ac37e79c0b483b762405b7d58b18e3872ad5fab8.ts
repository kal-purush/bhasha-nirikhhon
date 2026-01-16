import { Injectable } from '@nestjs/common';
import { CandidateInfo } from './entities/candidate-info.entity';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { CreateCandidateInfoDto } from './dto/create-candidate-info.dto.js';
import { UpdateCandidateInfoDto } from './dto/update-candidate-info.dto.js';
import { UserBasedService } from '../../src/utils/UserBasedService.js';

@Injectable()
export class CandidateInfoService extends UserBasedService<
  CandidateInfo,
  CreateCandidateInfoDto,
  UpdateCandidateInfoDto
> {
  constructor(
    @InjectRepository(CandidateInfo)
    candidateInfoRepository: Repository<CandidateInfo>,
  ) {
    super(CandidateInfo, candidateInfoRepository, { entityName: 'Candidate Info', relations: { user: true } });
  }
}