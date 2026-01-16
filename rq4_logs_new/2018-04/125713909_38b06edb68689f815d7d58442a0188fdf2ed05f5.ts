import { Component, OnInit, Input, Output, EventEmitter, OnChanges, SimpleChanges } from '@angular/core';
import { BsModalRef } from 'ngx-bootstrap/modal/bs-modal-ref.service';
import { BsModalService } from 'ngx-bootstrap/modal';
import {
  DynamicFormControlModel, DynamicInputModel,
  DynamicRadioGroupModel, DynamicCheckboxModel, DynamicFormService, DynamicDatePickerModel, DynamicSelectModel, DynamicFormGroupModel
} from '@ng-dynamic-forms/core';
import { FormGroup } from '@angular/forms';

@Component({
  selector: 'app-treatment-table',
  templateUrl: './treatment-table.component.html',
  styleUrls: ['./treatment-table.component.css']
})
export class TreatmentTableComponent implements OnInit {
  @Input() group: DynamicFormGroupModel;
  @Input() formGroup: FormGroup;
  @Input() treatmentName: string;
  @Input() set treatments(value: any[]) {
    if (value) {
      this.tempTreatments = value.slice();
    }
  }
  @Output() tableChange = new EventEmitter<any>();

  tempTreatments: any[];
  selectedTreatmentIndex: any = null;
  currentlyEditedTreatment: any = null;

  modalRef: BsModalRef;
  constructor(private modalService: BsModalService, private formService: DynamicFormService) { }

  ngOnInit() {
    this.formGroup = this.formService.createFormGroup([this.group]);
    this.modalService.config.backdrop = 'static';
  }

  onInputChange(e) {
    this.currentlyEditedTreatment[e.model.id] = e.control.value;
  }

  onEdit(treatment, template, index) {
    this.selectedTreatmentIndex = index;
    this.currentlyEditedTreatment = Object.assign({}, treatment);
    Object.getOwnPropertyNames(this.formGroup.controls).forEach(id => {
      const group: FormGroup = this.formGroup.controls[id] as FormGroup;
      Object.getOwnPropertyNames(group.controls).forEach(key => {
        group.controls[key].patchValue(this.currentlyEditedTreatment[key]);
      });
    });
    this.modalRef = this.modalService.show(template);
  }

  onAdd() {
    this.tempTreatments.push({});
    this.tableChange.emit({ treatmentName: this.treatmentName, treatments: this.tempTreatments });
  }

  onRemove(index) {
    this.tempTreatments.splice(index, 1);
    this.tableChange.emit({ treatmentName: this.treatmentName, treatments: this.tempTreatments });
  }

  onTreatmentSave() {
    this.tempTreatments[this.selectedTreatmentIndex] = this.currentlyEditedTreatment;
    this.currentlyEditedTreatment = null;
    this.tableChange.emit({ treatmentName: this.treatmentName, treatments: this.treatments });
    this.modalRef.hide();
  }

  onTreatmentCancel() {
    this.currentlyEditedTreatment = null;
    this.modalRef.hide();
  }
}