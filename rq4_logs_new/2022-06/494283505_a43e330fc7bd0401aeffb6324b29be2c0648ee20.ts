import { ApiProperty } from '@nestjs/swagger';
import { User } from 'src/user/entities/user.entity';

export class LoginResponseDto {
  @ApiProperty({
    description: 'JWT gerado pelo login',
  })
  token: string;
  @ApiProperty({
    description: 'Dados do usuário autenticado',
  })
  user: User;
}