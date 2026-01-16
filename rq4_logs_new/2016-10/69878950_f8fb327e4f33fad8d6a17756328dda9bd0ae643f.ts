import {Component, OnInit} from "@angular/core";
import {Http} from "@angular/http";

@Component({
    selector: 'app',
    templateUrl: 'app/app.component.html',
})
export class AppComponent implements OnInit {

    private data;

    selectedEntities: any[];

    constructor(private http: Http) {
    }

    ngOnInit(): void {
        this.http.get("/app/data.json")
            .subscribe((data)=> {
                setTimeout(()=> {
                    this.data = data.json();
                }, 5000);
            });
    }

    public setSelectedEntities($event) {
        this.selectedEntities = $event;
    }

    private toInt(num: string) {
        return +num;
    }

    private sortByWordLength = (a: any) => {
        return a.name.length;
    }

}