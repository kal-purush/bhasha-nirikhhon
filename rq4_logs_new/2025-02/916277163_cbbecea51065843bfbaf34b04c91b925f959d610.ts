import {
  IsBoolean,
  IsEmpty,
  IsEnum,
  IsNotEmpty,
  IsNumber,
  IsPositive,
  MaxLength,
} from 'class-validator';

import { EExpenseType } from '@repo/business/finance/enum';

import { ExpenseGroup } from '../expense-group/expense-group.entity';
import { Supplier } from '../../supplier/supplier.entity';
import { ExpenseCategory } from '../expense-category/expense-category.entity';
import { User } from '../../../auth/users/user.entity';

export class EntityExpenseDto {
  @IsNumber({ maxDecimalPlaces: 0 })
  year?: number;

  @IsEmpty()
  user: User;

  @IsNotEmpty()
  @IsEnum(EExpenseType)
  type: EExpenseType;

  @IsNotEmpty()
  @MaxLength(200)
  group: string | ExpenseGroup;

  @IsNotEmpty()
  @MaxLength(200)
  supplier: string | Supplier;

  @IsNotEmpty()
  @MaxLength(200)
  category: string | ExpenseCategory;

  @IsPositive()
  @IsEmpty()
  @IsNumber({ maxDecimalPlaces: 0 })
  january?: number;

  @IsBoolean()
  @IsEmpty()
  january_paid?: boolean;

  @IsPositive()
  @IsEmpty()
  @IsNumber({ maxDecimalPlaces: 0 })
  february?: number;

  @IsBoolean()
  @IsEmpty()
  february_paid?: boolean;

  @IsPositive()
  @IsEmpty()
  @IsNumber({ maxDecimalPlaces: 0 })
  march?: number;

  @IsBoolean()
  @IsEmpty()
  march_paid?: boolean;

  @IsPositive()
  @IsEmpty()
  @IsNumber({ maxDecimalPlaces: 0 })
  april?: number;

  @IsBoolean()
  @IsEmpty()
  april_paid?: boolean;

  @IsPositive()
  @IsEmpty()
  @IsNumber({ maxDecimalPlaces: 0 })
  may?: number;

  @IsBoolean()
  @IsEmpty()
  may_paid?: boolean;

  @IsPositive()
  @IsEmpty()
  @IsNumber({ maxDecimalPlaces: 0 })
  june?: number;

  @IsBoolean()
  june_paid?: boolean;

  @IsPositive()
  @IsEmpty()
  @IsNumber({ maxDecimalPlaces: 0 })
  july?: number;

  @IsBoolean()
  @IsEmpty()
  july_paid?: boolean;

  @IsPositive()
  @IsEmpty()
  @IsNumber({ maxDecimalPlaces: 0 })
  august?: number;

  @IsBoolean()
  @IsEmpty()
  august_paid?: boolean;

  @IsPositive()
  @IsEmpty()
  @IsNumber({ maxDecimalPlaces: 0 })
  september?: number;

  @IsBoolean()
  @IsEmpty()
  september_paid?: boolean;

  @IsPositive()
  @IsEmpty()
  @IsNumber({ maxDecimalPlaces: 0 })
  october?: number;

  @IsBoolean()
  @IsEmpty()
  october_paid?: boolean;

  @IsPositive()
  @IsEmpty()
  @IsNumber({ maxDecimalPlaces: 0 })
  november?: number;

  @IsBoolean()
  @IsEmpty()
  november_paid?: boolean;

  @IsPositive()
  @IsEmpty()
  @IsNumber({ maxDecimalPlaces: 0 })
  december?: number;

  @IsBoolean()
  @IsEmpty()
  december_paid?: boolean;

  @MaxLength(200)
  @IsEmpty()
  description?: string;

  @IsPositive()
  @IsEmpty()
  @IsNumber({ maxDecimalPlaces: 0 })
  instalment_number?: number;
}