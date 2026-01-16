import { Injectable } from '@angular/core';
import { MatPaginatorIntl } from '@angular/material/paginator';

@Injectable()
export class CustomMatPaginator extends MatPaginatorIntl {

  getRangeLabel = function (page, pageSize, length) {
    let estimatedLength;
    if (length < 1000) {
      estimatedLength = length;
    } else if (length < 10000) {
      estimatedLength = "1 000";
    } else if (length < 100000) {
      estimatedLength = "10 000";
    } else {
      estimatedLength = "100 000";
    }
    const start = page * pageSize + 1;
    const end = (page + 1) * pageSize;
    return `${start} – ${end} of ${estimatedLength} +`;
  };

}