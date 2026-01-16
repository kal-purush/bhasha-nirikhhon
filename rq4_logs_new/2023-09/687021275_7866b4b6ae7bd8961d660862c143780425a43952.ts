import {
  ArgumentMetadata,
  BadRequestException,
  PipeTransform,
} from '@nestjs/common';
import { PatchStreamDto } from 'src/dto';
import { PatchStreamSchema } from 'src/schema';

export class PatchQueryValidatorPipe implements PipeTransform {
  public transform(query: PatchStreamDto, metadata: ArgumentMetadata) {
    const result = PatchStreamSchema.validate(query, {
      convert: true,
    });

    if (result.error) {
      const errorMessages = result.error.details.map((d) => d.message).join();
      throw new BadRequestException(errorMessages);
    }

    return query;
  }
}