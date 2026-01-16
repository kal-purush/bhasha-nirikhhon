import { HttpException, HttpStatus } from '@nestjs/common';

export class DDAPIError extends HttpException {
  constructor() {
    super('Error to fetch data from D&D API', HttpStatus.INTERNAL_SERVER_ERROR);
  }
}