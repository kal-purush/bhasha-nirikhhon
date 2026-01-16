import { IsNotEmpty, IsString } from 'class-validator';

export class SyncRealtimeRequestDto {
  @IsString()
  @IsNotEmpty()
  syncRealtimeEvent: string;
  isNew: boolean;
  @IsString()
  @IsNotEmpty()
  referenceId: string;
}