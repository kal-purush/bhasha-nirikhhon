import { ConflictException } from '@nestjs/common';

export class dataBaseException extends ConflictException {
  constructor(msg) {
    super(msg);
  }
}