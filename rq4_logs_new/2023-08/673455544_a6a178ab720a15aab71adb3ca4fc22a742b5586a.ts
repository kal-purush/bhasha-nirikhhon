import { Injectable, CanActivate, ExecutionContext} from '@nestjs/common';
import { Request } from 'express';
import { UnauthorizedException } from '@nestjs/common';
import { User } from 'src/modules/users/entities/user.entity';
import { PrismaService } from 'src/database/prisma.service';



@Injectable()
export class ImagePermissionGuard implements CanActivate {
  constructor(private prisma: PrismaService) {}
  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest<Request>();

    const imageId = request.params.id;

    const user: User = request.user as User;
    
    const userId = user.id;

    const cars = await this.prisma.car.findMany({
        where: {userId: userId}
    });
    if(cars.length == 0){
      throw new UnauthorizedException("you do not have permission to perform this task");
    }
    for(let i = 0; i < cars.length; i++){
        const image = await this.prisma.image.findFirst({
          where: {carId: cars[i].id, id: imageId}
        })
        if(!image){
          throw new UnauthorizedException("you do not have permission to perform this task");
        } 
        return true;
    }
  }
}