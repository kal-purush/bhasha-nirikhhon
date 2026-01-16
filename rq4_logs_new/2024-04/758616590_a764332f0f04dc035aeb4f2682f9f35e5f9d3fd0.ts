import { IsNotEmpty, IsNumber } from 'class-validator';

//#region reportMaster

export class CreateReportMasterDto {
  id: number;
  @IsNotEmpty()
  report_name: string;
  report_path: string;
  report_destination: string;
  @IsNotEmpty()
  customer_id: number;
  created_on: Date;
  created_by: number;
}

export class UpdateReportMasterDto {
  @IsNotEmpty()
  id: number;
  report_name: string;
  report_destination: string;
  report_path: string;
  @IsNotEmpty()
  customer_id: number;
  changed_on: Date;
  changed_by: number;
}

export class ReadReportMasterDto {
  id: number;
  report_name: string;
  report_destination: string;
  report_path: string;
  is_active: string;
  created_on: Date;
  created_by: number;
  changed_on: Date;
  changed_by: number;
  customer_id: number;
}

export class DeleteReportMasterDto {
  @IsNotEmpty()
  id: number;
  @IsNotEmpty()
  customer_id: number;
  changed_on: Date;
  changed_by: number;
}
//#endregion reportMaster