import { Injectable } from '@nestjs/common';
import { CreateGroupDto } from '../group/dto/creategroup.dto';
import CreateGroupMemberDto from './dto/creategroupmember.dto';
import GroupMemberRepository from './groupmember.repository';

@Injectable()
export class GroupmemberService {
  constructor(private readonly groupMemberRepository: GroupMemberRepository) {}

  /**
   * Create user config for a group
   * @param createGroupOptions - parameters needed to create the groupmember config
   */
  async create(createGroupOptions: CreateGroupMemberDto) {
    const { group, role, user } = createGroupOptions;
    const groupConfig = this.groupMemberRepository.create({ role, group, user });

    await this.groupMemberRepository.save(groupConfig);
  }
}