import { Body, Controller, HttpCode, HttpStatus, Logger, Post } from '@nestjs/common';
import { ApiTags, ApiCookieAuth, ApiBadRequestResponse, ApiCreatedResponse, ApiOperation } from '@nestjs/swagger';
import { InjectEntityManager } from '@nestjs/typeorm';
import { Role } from 'src/auth/roles/role.enum';
import { Roles } from 'src/auth/roles/roles.decorator';
import { ReferralsService } from 'src/referrals/referrals.service';
import { CreateUserDto } from 'src/users/dto/create-user.dto';
import { ResponseUserDto } from 'src/users/dto/response-user.dto';
import { User } from 'src/users/entities/user.entity';
import { UsersService } from 'src/users/users.service';
import { EntityManager } from 'typeorm';

@Controller('admin')
@ApiTags('admin')
@ApiCookieAuth()
export class AdminController {
  private readonly logger = new Logger(AdminController.name);

  constructor(
    private usersService: UsersService,
    private referralService: ReferralsService,
    @InjectEntityManager()
    private entityManager: EntityManager,
  ) {}

  @Post('users')
  @HttpCode(HttpStatus.CREATED)
  @Roles(Role.ADMIN)
  @ApiOperation({ summary: 'Creates user and sends invitation' })
  @ApiCreatedResponse({ type: ResponseUserDto })
  @ApiBadRequestResponse()
  async createUser(@Body() dto: CreateUserDto): Promise<ResponseUserDto> {
    let user: User;
    await this.entityManager.transaction(async (entityManager) => {
      user = await this.usersService.createUser(dto, { entityManager });
      const referral = await this.referralService.createReferral(user, { entityManager });
      // TODO send invitation email
      this.logger.log(`Generated referralId="${referral.referralId}" for user="${dto.email}"`);
    });
    return ResponseUserDto.fromEntity(user);
  }
}