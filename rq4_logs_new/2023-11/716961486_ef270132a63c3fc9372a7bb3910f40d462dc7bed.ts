import { IsDateString, IsNotEmpty, IsNumber } from 'class-validator';

export class CreateAvailableAppointmentDto {
  @IsNumber()
  @IsNotEmpty()
  doctorId: number;

  @IsNotEmpty()
  availableSlots: AppointmentDetails[];
}
class AppointmentDetails {
  @IsDateString()
  @IsNotEmpty()
  dateTime: Date;
}