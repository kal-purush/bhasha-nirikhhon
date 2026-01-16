import { Pipe, PipeTransform } from '@angular/core';
import { OptionDto } from 'src/app/DTOs/OptionDto';
import { OptionValueDto } from 'src/app/DTOs/OptionValueDto';

@Pipe({
  name: 'filterOptinValueNotSelected',
})
export class FilterOptinValueNotSelectedPipe implements PipeTransform {
  transform(
    options: OptionDto[],
    optionProductIdSelected: number,
    optionValuesSelected: OptionValueDto[]
  ): OptionValueDto[] {
    let optionValues = [
      ...(options.find((option) => option.id == optionProductIdSelected)
        ?.optionValueDtos as OptionValueDto[]),
    ];
    return optionValues.filter(
      (optionValue) => !optionValuesSelected.some((o) => o.id == optionValue.id)
    );
  }
}