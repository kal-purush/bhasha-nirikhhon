import {IAmountOfFee} from "../entities/AmountOfFee";

export function findAllAmountOfFees(): IAmountOfFee[] {
    return [
        {
            year: 2019,
            costCenter: 'CS 153',
            totalAmount: 3500,
            lawFirm: 'Clifford chance',
        },
        {
            year: 2018,
            costCenter: 'CS 47',
            totalAmount: 9500,
            lawFirm: 'Linklasters',
        },
        {
            year: 2017,
            costCenter: 'CS 32',
            totalAmount: 10500,
            lawFirm: 'Linklasters',
        },
        {
            year: 1945,
            costCenter: 'CS 153',
            totalAmount: 18500,
            lawFirm: 'Linklasters',
        },
        {
            year: 1945,
            costCenter: 'CS 153',
            totalAmount: 15500,
            lawFirm: 'Linklasters',
        },
    ];
}