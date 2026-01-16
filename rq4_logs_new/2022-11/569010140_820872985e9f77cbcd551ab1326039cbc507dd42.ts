import { Pipe, PipeTransform } from "@angular/core";

@Pipe({
    name: "spacing"
})
export class PersoPipe implements PipeTransform {
    
    transform(megadada: any, args?: any): any {
        let texto:String;
        if (megadada != null) {
             texto = megadada.toUpperCase();
             texto = texto.slice(0,15)
            console.log(texto);
            return texto;
        }
        
    }
}