import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
    name: 'transType'
})
export class TransactionType implements PipeTransform {
    transform(value: any) {
        if (value === 0) {
            return 'Immediate';
        } else if (value === 1) {
            return 'Future';
        } else if (value === 2) {
            return 'Recurring';
        } else {
            return value;
        }
    }
}