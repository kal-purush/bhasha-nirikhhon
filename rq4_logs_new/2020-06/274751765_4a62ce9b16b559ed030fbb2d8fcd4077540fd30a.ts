import { Component, VERSION } from "@angular/core";
import { ValueItem } from "./model";

@Component({
  selector: "my-app",
  templateUrl: "./app.component.html",
  styleUrls: ["./app.component.css"]
})
export class AppComponent {
  get valueString() {
    return JSON.stringify(this.valueList, null, 2);
  }
  valueList: Array<ValueItem> = [
    ];

    logData() {
      console.dir(this.valueList);
    }
}