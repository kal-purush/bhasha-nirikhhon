import { Component, Input, Output, EventEmitter, OnInit, OnChanges, SimpleChanges } from '@angular/core';
import { select } from "@angular-redux/store";
import { ActivatedRoute } from "@angular/router";
import { Observable } from "rxjs/Observable";
import { FormControl, FormGroup } from '@angular/forms';

@Component({
    selector: "app-form-data",
    templateUrl: "./form-data.component.html",
  })
export class FormDataComponent implements OnInit, OnChanges {
    @Input() public algemeenData;
    @Input() public bestemmelingenData;
    @Input() public metadataData;

    public dataForm: FormGroup;

    public onSubmit(){
      console.log(JSON.stringify(this.dataForm.value, null, 2));
    }

    public ngOnInit() {
      this.dataForm = new FormGroup({
        algemeen: this.createForm(this.algemeenData),
        bestemmelingen: this.createForm(this.bestemmelingenData),
        metadata: this.createForm(this.metadataData),
      });
    }

    public ngOnChanges() {}

    createForm(formData){
      return new FormGroup(formData.reduce((result, current) => {
        return {
          ...result,
          [current.key]: new FormControl(current.value)
        };
      }, {}));

    }
}