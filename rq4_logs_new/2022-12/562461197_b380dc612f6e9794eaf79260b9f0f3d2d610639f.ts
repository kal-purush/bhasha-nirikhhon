import { Component, ViewEncapsulation } from '@angular/core';
import { TypeaheadMatch } from 'ngx-bootstrap/typeahead';
import { UsState } from 'src/app/shared/models/shared.model';

@Component({
  selector: 'app-form-validation-content',
  templateUrl: './ngxb.typeahead.preview.html',
  styleUrls: ['./ngxb.typeahead.preview.scss', '../../../../assets/scss/bootstrap.min.css'],
  // encapsulation: ViewEncapsulation.None,
  encapsulation: ViewEncapsulation.ShadowDom,
  // encapsulation: ViewEncapsulation.Emulated,
})
export class NgxbTypeaheadPreviewComponent {
  selectedValue?: string;
  selectedOption: UsState;
  previewOption?: UsState;
  states: UsState[] = [
    { id: 1, stateName: 'Alabama', region: 'South' },
    { id: 2, stateName: 'Alaska', region: 'West' },
    { id: 3, stateName: 'Arizona', region: 'West' },
    { id: 4, stateName: 'Arkansas', region: 'South' },
    { id: 5, stateName: 'California', region: 'West' },
    { id: 6, stateName: 'Colorado', region: 'West' },
    { id: 7, stateName: 'Connecticut', region: 'Northeast' },
    { id: 8, stateName: 'Delaware', region: 'South' },
    { id: 9, stateName: 'Florida', region: 'South' },
    { id: 10, stateName: 'Georgia', region: 'South' },
    { id: 11, stateName: 'Hawaii', region: 'West' },
    { id: 12, stateName: 'Idaho', region: 'West' },
    { id: 13, stateName: 'Illinois', region: 'Midwest' },
    { id: 14, stateName: 'Indiana', region: 'Midwest' },
    { id: 15, stateName: 'Iowa', region: 'Midwest' },
    { id: 16, stateName: 'Kansas', region: 'Midwest' },
    { id: 17, stateName: 'Kentucky', region: 'South' },
    { id: 18, stateName: 'Louisiana', region: 'South' },
    { id: 19, stateName: 'Maine', region: 'Northeast' },
    { id: 21, stateName: 'Maryland', region: 'South' },
    { id: 22, stateName: 'Massachusetts', region: 'Northeast' },
    { id: 23, stateName: 'Michigan', region: 'Midwest' },
    { id: 24, stateName: 'Minnesota', region: 'Midwest' },
    { id: 25, stateName: 'Mississippi', region: 'South' },
    { id: 26, stateName: 'Missouri', region: 'Midwest' },
    { id: 27, stateName: 'Montana', region: 'West' },
    { id: 28, stateName: 'Nebraska', region: 'Midwest' },
    { id: 29, stateName: 'Nevada', region: 'West' },
    { id: 30, stateName: 'New Hampshire', region: 'Northeast' },
    { id: 31, stateName: 'New Jersey', region: 'Northeast' },
    { id: 32, stateName: 'New Mexico', region: 'West' },
    { id: 33, stateName: 'New York', region: 'Northeast' },
    { id: 34, stateName: 'North Dakota', region: 'Midwest' },
    { id: 35, stateName: 'North Carolina', region: 'South' },
    { id: 36, stateName: 'Ohio', region: 'Midwest' },
    { id: 37, stateName: 'Oklahoma', region: 'South' },
    { id: 38, stateName: 'Oregon', region: 'West' },
    { id: 39, stateName: 'Pennsylvania', region: 'Northeast' },
    { id: 40, stateName: 'Rhode Island', region: 'Northeast' },
    { id: 41, stateName: 'South Carolina', region: 'South' },
    { id: 42, stateName: 'South Dakota', region: 'Midwest' },
    { id: 43, stateName: 'Tennessee', region: 'South' },
    { id: 44, stateName: 'Texas', region: 'South' },
    { id: 45, stateName: 'Utah', region: 'West' },
    { id: 46, stateName: 'Vermont', region: 'Northeast' },
    { id: 47, stateName: 'Virginia', region: 'South' },
    { id: 48, stateName: 'Washington', region: 'South' },
    { id: 49, stateName: 'West Virginia', region: 'South' },
    { id: 50, stateName: 'Wisconsin', region: 'Midwest' },
    { id: 51, stateName: 'Wyoming', region: 'West' },
  ];

  onSelect(event: TypeaheadMatch): void {
    this.selectedOption = event.item;
  }

  onPreview(event: TypeaheadMatch): void {
    if (event) {
      this.previewOption = event.item;
    } else {
      this.previewOption = null;
    }
  }
}