import { ApiProperty } from '@nestjs/swagger';
import { Car, CarsPagination } from '../entities/car.entity';
import { CommentsSwagger } from 'src/modules/comments/swagger/comments.swagger';
import { ImageSwagger } from 'src/modules/images/swagger/images.swagger';
import { UserSwagger } from 'src/modules/users/swagger/users.swagger';

export class CarSwagger extends Car {}

export class CarRelationsSwagger extends Car {
  @ApiProperty({ type: ImageSwagger, isArray: true })
  images: ImageSwagger[];

  @ApiProperty({ type: CommentsSwagger, isArray: true })
  comments: CommentsSwagger[];

  @ApiProperty({ type: () => UserSwagger })
  user: UserSwagger[];
}

export class CarsPaginationSwagger extends CarsPagination {
  @ApiProperty({ type: CarRelationsSwagger, isArray: true })
  cars: CarRelationsSwagger[];
}