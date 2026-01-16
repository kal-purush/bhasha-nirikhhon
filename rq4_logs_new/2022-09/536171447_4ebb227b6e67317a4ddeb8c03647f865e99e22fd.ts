import {
  PipeTransform,
  Injectable,
  ArgumentMetadata,
  BadRequestException,
} from '@nestjs/common';

@Injectable()
export class ParseIntMinMaxPipe implements PipeTransform<string, number> {
  private readonly min: number;
  private readonly max: number;
  constructor(min: number, max: number) {
    this.min = min;
    this.max = max;
  }
  transform(value: string, metadata: ArgumentMetadata): number | null {
    const val = Math.floor(parseInt(value));
    if (isNaN(val) || val < this.min || val > this.max) {
      throw new BadRequestException('Validation failed');
    }
    return val;
  }
}