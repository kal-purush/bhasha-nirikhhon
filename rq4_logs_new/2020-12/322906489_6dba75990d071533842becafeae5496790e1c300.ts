import { Pipe, PipeTransform } from '@angular/core';


@Pipe({
    name: 'dateStr'
})
export class DateStrPipe implements PipeTransform {
    transform(value: any, full = false) {
        const components = value.split('-');
        const year = components[0];
        const month = components[1];
        const day = components[2];
        let monthStr = '';

        switch (month) {
            case '01':
                monthStr = full ? 'January' : 'Jan';
                break;
            case '02':
                monthStr = full ? 'Feburary' : 'Feb';
                break;
            case '03':
                monthStr = full ? 'March' : 'Mar';
                break;
            case '04':
                monthStr = full ? 'April' : 'Apr';
                break;
            case '05':
                monthStr = full ? 'May' : 'May';
                break;
            case '06':
                monthStr = full ? 'June' : 'Jun';
                break;
            case '07':
                monthStr = full ? 'July' : 'Jul';
                break;
            case '08':
                monthStr = full ? 'August' : 'Aug';
                break;
            case '09':
                monthStr = full ? 'September' : 'Sep';
                break;
            case '10':
                monthStr = full ? 'October' : 'Oct';
                break;
            case '11':
                monthStr = full ? 'November' : 'Nov';
                break;
            case '12':
                monthStr = full ? 'December' : 'Dec';
                break;
        }

        return monthStr + ' ' + day + ', ' + year;
    }
}