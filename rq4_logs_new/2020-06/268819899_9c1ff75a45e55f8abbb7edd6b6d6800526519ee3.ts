import { Component } from "@angular/core";

class outputData {
  lstPyramidOn = new Array();
  lstPyramidOff = new Array();
}

@Component({
  selector: "year",
  templateUrl: "./year.html",
})
export class year {
  outputDataObject = new outputData();
  lstPyramidOff: string;
  clickme() {
    console.log("outputDataObject", this.outputDataObject);
  }
  onChange(event) {
    let Eventvalue = event.target.id.substring(0, event.target.id.length - 1);
    let index = event.target.id.substring(event.target.id.length - 1);
    if (Eventvalue === "yearOn") {
      this.outputDataObject.lstPyramidOn[index] = event.target.value;
      this.outputDataObject.lstPyramidOff[index] = 100 - event.target.value;
    } else if (Eventvalue === "yearOff") {
      this.outputDataObject.lstPyramidOff[index] = event.target.value;
      this.outputDataObject.lstPyramidOn[index] = 100 - event.target.value;
    }
  }
}