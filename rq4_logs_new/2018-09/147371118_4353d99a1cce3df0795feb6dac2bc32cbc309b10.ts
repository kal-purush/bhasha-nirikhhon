import { Component, OnInit, ViewChild } from '@angular/core';
import { MatPaginator, MatTableDataSource } from '@angular/material';

import { environment as env } from '@env/environment';
import { ROUTE_ANIMATIONS_ELEMENTS } from '@app/core';

@Component({
  selector: 'anms-student',
  templateUrl: './student.component.html',
  styleUrls: ['./student.component.scss']
})
export class StudentComponent implements OnInit {
  routeAnimationsElements = ROUTE_ANIMATIONS_ELEMENTS;
  versions = env.versions;
  displayedColumns: string[] = ['school_code', 'school_year', 'student', 'course_code'];
  dataSource = new MatTableDataSource<PeriodicElement>(ELEMENT_DATA);
  @ViewChild(MatPaginator) paginator: MatPaginator;

  ngOnInit() {
    this.dataSource.paginator = this.paginator;
  }

  openLink(link: string) {
    window.open(link, '_blank');
  }
}

export interface PeriodicElement {
  school_code: number;
  school_year: string;
  student: number;
  course_code: string;
}

const ELEMENT_DATA: PeriodicElement[] = [
  { school_code: 1, school_year: 'Hydrogen', student: 1.0079, course_code: 'H' },
  { school_code: 2, school_year: 'Helium', student: 4.0026, course_code: 'He' },
  { school_code: 3, school_year: 'Lithium', student: 6.941, course_code: 'Li' },
  { school_code: 4, school_year: 'Beryllium', student: 9.0122, course_code: 'Be' },
  { school_code: 5, school_year: 'Boron', student: 10.811, course_code: 'B' },
  { school_code: 6, school_year: 'Carbon', student: 12.0107, course_code: 'C' },
  { school_code: 7, school_year: 'Nitrogen', student: 14.0067, course_code: 'N' },
  { school_code: 8, school_year: 'Oxygen', student: 15.9994, course_code: 'O' },
  { school_code: 9, school_year: 'Fluorine', student: 18.9984, course_code: 'F' },
  { school_code: 10, school_year: 'Neon', student: 20.1797, course_code: 'Ne' },
  { school_code: 11, school_year: 'Sodium', student: 22.9897, course_code: 'Na' },
  { school_code: 12, school_year: 'Magnesium', student: 24.305, course_code: 'Mg' },
  { school_code: 13, school_year: 'Aluminum', student: 26.9815, course_code: 'Al' },
  { school_code: 14, school_year: 'Silicon', student: 28.0855, course_code: 'Si' },
  { school_code: 15, school_year: 'Phosphorus', student: 30.9738, course_code: 'P' },
  { school_code: 16, school_year: 'Sulfur', student: 32.065, course_code: 'S' },
  { school_code: 17, school_year: 'Chlorine', student: 35.453, course_code: 'Cl' },
  { school_code: 18, school_year: 'Argon', student: 39.948, course_code: 'Ar' },
  { school_code: 19, school_year: 'Potassium', student: 39.0983, course_code: 'K' },
  { school_code: 20, school_year: 'Calcium', student: 40.078, course_code: 'Ca' },
];