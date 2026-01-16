import {Pipe, Injectable, PipeTransform} from "angular2/core";
import {Link} from "./link";
@Pipe({
    name: "linkfilter",
    pure: false
})
@Injectable()
export class LinkFilter implements PipeTransform {
    transform(items:Link[], args:string[]):Link[] {
        // filter items array, items which match and return true will be kept, false will be filtered out
        console.log(items, args);
        if (args[0] === undefined || items === undefined){
            return items;
        }
        var filterBy = args[0].toLowerCase();
        return items.filter(item => item.name.toLowerCase().includes(filterBy));
    }
}