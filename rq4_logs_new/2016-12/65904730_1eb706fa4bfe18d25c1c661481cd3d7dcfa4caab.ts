import { Component, Input, OnChanges, SimpleChanges } from '@angular/core';


@Component({
    selector:'percent-display',
    template: `
        <span style="text-shadow:-1px -1px 0 #000,
    1px -1px 0 #000,
    -1px 1px 0 #000,
     1px 1px 0 #000;" [style.color]="getColorString()"> {{percent}} %</span>
    `
})
export class PercentDisplayComponent implements OnChanges {
    @Input() value: number;

    color: string = '';
    percent: number = 0;

    ngOnChanges(...args: any[]){
        this.percent = Math.floor(args[0].value.currentValue * 20);
        console.log(this.percent + " prosentti");

    }

    getColorString(){
        let color = '';
        let part = (this.percent > 50) ? Math.floor(255 * ((100 - this.percent) / 50)) : Math.floor(255 * (this.percent / 50));

        if (this.percent < 50) {
            color = '(255,' + part + ',0)'
        } else if ( this.percent > 50){
            color = '(' + part + ',255, 0)';
        } else {
            color = '(255,255,0)';
        }

        console.log("color= " + color);
        return 'rgb' + color;
    }



}