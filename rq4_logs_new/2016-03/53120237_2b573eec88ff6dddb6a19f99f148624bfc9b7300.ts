import {Pipe, PipeTransform} from 'angular2/core';

@Pipe({
    name:'capitalizeFirstLetter'
})

export class CapitalizeFirstLetterPipe implements PipeTransform {
    transform(value:string, args :string[]) {
        var result:string ='';
        result = (value.toLowerCase().replace(/\b[a-z]/g, function(letter) {
            return letter.toUpperCase();
        }));
        return result;
    }
}