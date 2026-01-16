import {Pipe} from "angular2/core";
import {PipeTransform} from "angular2/core";
/**
 * Created by Lawrence on 4/16/16.
 */
@Pipe({
    name: 'myReverse'
})
export class ReversePipe implements PipeTransform {

    transform(value:any, args:any[]):any {
        const length = value.length;
        let resultString = '';

        for (let i = 0; i < length; i++) {
            resultString = value[i] + resultString;
        }

        return resultString;
    }

}