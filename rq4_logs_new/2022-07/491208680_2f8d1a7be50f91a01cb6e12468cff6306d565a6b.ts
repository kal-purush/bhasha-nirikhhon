import { ApiProperty } from "@nestjs/swagger";


export class TotalByUserResponse {
  @ApiProperty()
  total: number;

  @ApiProperty()
  userFree: number;

  @ApiProperty()
  userPremium: number;

  @ApiProperty()
  admin: number;

  @ApiProperty()
  sensor: number;


  constructor(userFree: number, userPremium: number, admin: number, sensor: number) {
    this.userFree = userFree;
    this.userPremium = userPremium;
    this.admin = admin;
    this.sensor = sensor;
    this.total = userFree + userPremium + admin + sensor;
  }
}