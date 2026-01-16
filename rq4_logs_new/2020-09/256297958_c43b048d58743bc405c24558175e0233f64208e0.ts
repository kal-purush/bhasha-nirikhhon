import { IntervalNotExistingException } from '../../../Shared/domain/exceptions/IntervalNotExistingException';

export enum IntervalPeriodicTransaction {
    DAY,
    WEEK,
    MONTH
}

export function numberToIntervalPeriodicTransaction(value: number): IntervalPeriodicTransaction {
    switch (value) {
        case 0:
            return IntervalPeriodicTransaction.DAY;
        case 1:
            return IntervalPeriodicTransaction.WEEK;
        case 2:
            return IntervalPeriodicTransaction.MONTH;
        default:
            throw new IntervalNotExistingException();
    }
}