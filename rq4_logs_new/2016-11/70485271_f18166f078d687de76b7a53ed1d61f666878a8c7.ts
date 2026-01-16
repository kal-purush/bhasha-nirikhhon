import { Component, OnInit } from "@angular/core";
import { ApiService } from "../../../service/api.service";

@Component({
    selector: "measure",
    templateUrl: `client/modules/measure/all/measure-all.component.html`
})
export class MeasureAllComponent implements OnInit {
    private measuresArray: any[];
    private typesArray: String[] = ['weight', 'volume'];
    private id: number;
    private showUpdate: boolean = false;
    private showCreate: boolean = true;
    private result = 0;
    private powerFirst:number;
    private powerSecond:number;
 

    constructor(private apiService: ApiService) { }
    ngOnInit() {
        this.apiService
            .getMeasures()
            .subscribe((data) => { this.measuresArray = data; });
    }

    updateMeasureValidate(id: number) {
        this.refresh();
        this.id = id;
        this.showUpdate = true;
        this.showCreate = false;
    }

    delete(id: number) {
        this.refresh();
        this.apiService
            .deleteMeasure(id)
            .subscribe(
            date => JSON.stringify(date),
            error => alert(error)
            );
    }

    createMeasure(name: String, type: String, power: number) {
        this.refresh();
        if (name.length > 0) {
            this
                .apiService
                .postCreateMeasure(name, type, power)
                .subscribe(
                date => console.log(date),
                error => alert(error)
                );
        } else {
            console.log("Name = null");
        }


    }

    updateMeasure(id: number, name: String, type: String, power: number) {
        this.showUpdate = false;
        this.showCreate = true;
        this.refresh();
        if (name.length > 0) {
            this
                .apiService
                .putUpdateMeasure(this.id, name, type, power)
                .subscribe(
                date => console.log(date),
                error => alert(error)
                );
        } else {
            console.log("Name = null");
        }
    }

    convertMeasure(firstValue: number, firstName: String, secondName: String) {
        
     for(let i =0;i< this.measuresArray.length;i++){
         if(this.measuresArray[i].name === firstName){
            this.powerFirst = this.measuresArray[i].power;
         }
         if(this.measuresArray[i].name === secondName){
            this.powerSecond = this.measuresArray[i].power;
         }
     }
     console.log(this.powerFirst+" " +this.powerSecond );
     this.result = (firstValue * this.powerFirst)/this.powerSecond;
    }

    refresh() {
        this.apiService
            .getMeasures()
            .subscribe((data) => { this.measuresArray = data; });
    }


}