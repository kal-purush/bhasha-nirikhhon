import { IsNotEmpty, IsNumber } from 'class-validator';


export class CreatePrivilegeMasterDto {
  id: number;
  @IsNotEmpty()
  privilege_name: string;
  privilege_desc: string;
  @IsNotEmpty()
  customer_id: number;
  created_on: Date;
  created_by: number;
}

export class UpdatePrivilegeMasterDto {
  @IsNotEmpty()
  id: number;
  privilege_name: string;
  privilege_desc: string;
  @IsNotEmpty()
  customer_id: number;
  changed_on: Date;
  changed_by: number;
}

export class ReadPrivilegeMasterDto {
  id: number;
  privilege_name: string;
  privilege_desc: string;
  is_active: string;
  created_on: Date;
  created_by: number;
  changed_on: Date;
  changed_by: number;
  customer_id: number;
}

export class DeletePrivilegeMasterDto {
  @IsNotEmpty()
  id: number;
  @IsNotEmpty()
  customer_id: number;
  changed_on: Date;
  changed_by: number;
}