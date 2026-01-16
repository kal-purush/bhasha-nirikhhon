import {Component} from 'angular2/core';
import {HTTPTestService} from "./http-test.service";

@Component({
    selector: 'http-test',
    template: `
        <button (click)="onTestGet()">Test Get Request</button>
        <p>Output: {{getData}}</p>
        <button (click)="onTestPost()">Test Post Request</button>
        <p>Output: {{postData}}</p>
        <button (click)="onTestPut()">Test Put Request</button>
        <button (click)="onTestDelete()">Test Delete Request</button>
        
    `,
    providers: [HTTPTestService]
})
export class HTTPTestComponent {
    getData: string;
    postData: string;

    constructor(private _httpService: HTTPTestService) {}

    onTestGet() {
        this._httpService.getEmployee()
            .subscribe(
                data => this.getData = JSON.stringify(data), //success part
                error => alert(error), //error case
                () => console.log("Finished") //pass a function when everything is completed
            );
    }

    onTestPost() {
        this._httpService.postEmployee()
            .subscribe(
                data => this.postData = JSON.stringify(data), //success part
                error => alert(error), //error case
                () => console.log("Finished") //pass a function when everything is completed
            );
    }

    onTestPut() {
        this._httpService.updateEmployee()
            .subscribe(
                data => this.postData = JSON.stringify(data), //success part
                error => alert(error), //error case
                () => console.log("Finished") //pass a function when everything is completed
            );
    }


    onTestDelete() {
        this._httpService.deleteEmployee()
            .subscribe(
                response => console.log(response),
                error => alert(error)
            );
    }
}