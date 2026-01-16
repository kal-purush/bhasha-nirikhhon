import { Injectable, CanActivate, ExecutionContext } from '@nestjs/common';
import { Request } from 'express';
import { UnauthorizedException } from '@nestjs/common';
import { User } from 'src/modules/users/entities/user.entity';
import { PrismaService } from 'src/database/prisma.service';

export class CarPermissionException extends UnauthorizedException {
  constructor() {
    super('Você não tem permissão para acessar este recurso');
  }
}

@Injectable()
export class CarPermissionGuard implements CanActivate {
  constructor(private prisma: PrismaService) {}
  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest<Request>();

    const carId = request.params.id;
    const user: User = request.user as User;
    const userId = user.id;

    const car = await this.prisma.car.findFirst({
        where: {id: carId, userId: userId}
    });
    if (!car) {
      throw new CarPermissionException();
    }
    return true;
  }
}