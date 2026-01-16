import { Controller } from '@nestjs/common';
import { ReadController } from './core/read.controller';
import { InjectModel } from '@nestjs/mongoose';
import { AccountType } from '../models/account-type';

@Controller('account-types')
export class AccountTypesController extends ReadController<ArrayBufferTypes> {
  constructor(@InjectModel(AccountType.name) model) { super(model); }
}