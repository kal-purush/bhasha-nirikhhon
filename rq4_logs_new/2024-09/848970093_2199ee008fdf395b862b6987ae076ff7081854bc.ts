import { Inject, Injectable } from '@nestjs/common';
import { AppError } from '../../../common/errors/Error';
import { AdminRepository } from '../repository/admin.repository';
import { IAdminRepository } from '../interfaces/repository.interface';
import { Admin } from '@prisma/client';
import { IAdmin } from '../interfaces/admin.interface';
import { SecurityService } from '../../../common/services/security.service';
import { UpdateAdminDto } from '../dto/update-admin.dto';
import { PrismaService } from 'src/prisma.service';

@Injectable()
export class UpdateAdminService {
  constructor(
    @Inject(AdminRepository)
    private readonly adminRepository: IAdminRepository<Admin>,
    private readonly securityService: SecurityService,
    private prisma: PrismaService,
  ) {}

  async execute(id: string, data: UpdateAdminDto): Promise<IAdmin> {
    try {
      if (data.password && data.passwordConfirmation && data.oldPassword) {
        if (data.password !== data.passwordConfirmation) {
          throw new AppError(
            'admin-service.updateAdmin',
            422,
            'new passwords do not match',
          );
        }

        const user = await this.prisma.admin.findFirst({
          where: { id: Number(id) },
        });

        if (user) {
          const isPasswordValid = await this.securityService.comparePasswords(
            data.oldPassword,
            user.password,
          );

          if (!isPasswordValid) {
            throw new AppError(
              'admin-service.updateAdmin',
              422,
              'invalid old password',
            );
          }
        }
      }

      delete data.passwordConfirmation, delete data.oldPassword;

      return await this.adminRepository.updateAdmin(id, data);
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }

      throw new AppError(
        'admin-service.findOneAdmin',
        500,
        'failed to get admin',
      );
    }
  }
}