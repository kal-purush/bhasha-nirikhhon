/**
 * Created by cookx876 on 2/13/17.
 */
import {Pipe, PipeTransform} from "@angular/core";

@Pipe({
    name: 'sortBy'
})

export class sortBy implements PipeTransform{
    private sortByString(keyword){
        keyword = keyword.toLowerCase();
        return value => {
            return !keyword || value.toLowerCase().indexOf(keyword) !== -1;
        }
    }

    transform(array: any[], keyword: string): any {
        if(array==null){
            return null;
        }
        const type = typeof keyword;

        if(type === 'string'){
            console.log("IMA STRING! WUTTTTT!");
            return array.filter(this.sortByString(keyword));
        }

        // if(type == 'object'){
        //     return array.filter(this.filterByObject(filter));
        // } else {
        //     console.log("Unknown filter type???");
        //     console.log(type);
        //     console.log(filter);
        //     return array;
        // }

    }

}