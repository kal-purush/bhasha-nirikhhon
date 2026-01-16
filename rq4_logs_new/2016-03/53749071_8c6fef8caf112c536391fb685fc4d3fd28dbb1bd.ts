
import {Component} from "angular2/core";
import {ButtonNumberComponent} from "./button.components/number.component";
import {CalculatorModel} from "./button.model/CalculatorModel";
import {OperationMap} from "./calculator.operation/OperationMap";
import {ButtonsService} from "./button.service/buttons.service";

@Component({
    providers: [CalculatorModel,ButtonsService] ,

    selector: 'calculator',
    template: `
    <div>
        <tr *ngFor="#array of buttonsArray">
            <td  *ngFor="#ob of array">
                <numberButton [numb]='ob' (resultChanged)="displayResult($event)"></numberButton>
            </td>
        </tr>
    </div>
    <input type="text" class="calcText" value="{{result}}">

    `,
    styleUrls:["../src/css/app.css"],
    directives:[ButtonNumberComponent]

})

export class AppComponent {
    private result:string;
    private buttonsArray;

    constructor(private model:CalculatorModel,
                private operationMap:OperationMap,
                private buttonsService:ButtonsService){ }


    ngOnInit() {
        this.getButtons();
    }



    getButtons(){
        this.buttonsService.getButtons()
            .subscribe(
                buttonsArray => this.buttonsArray = buttonsArray,
                error => console.error('Error: ' + error),
                () =>  this.buttonsArray[0]);
    }


    displayResult(numb){
        this.result = this.model.setValue(numb,this.operationMap);
    }



}