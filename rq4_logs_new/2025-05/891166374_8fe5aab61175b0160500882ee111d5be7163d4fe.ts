import { IsNotEmpty, IsString, IsUUID, IsOptional, IsEnum, IsArray } from 'class-validator';
import { ChatRoomType } from 'src/domain/entities/chat-room.entity';

export class CreateRoomDto {
  @IsNotEmpty()
  @IsString()
  name: string;

  @IsNotEmpty()
  @IsEnum(ChatRoomType)
  type: ChatRoomType;

  @IsOptional()
  @IsUUID()
  supplierId?: string;

  @IsOptional()
  @IsArray()
  @IsUUID('4', { each: true })
  employeeIds?: string[];

  @IsOptional()
  @IsArray()
  @IsUUID('4', { each: true })
  visitorIds?: string[];
} 