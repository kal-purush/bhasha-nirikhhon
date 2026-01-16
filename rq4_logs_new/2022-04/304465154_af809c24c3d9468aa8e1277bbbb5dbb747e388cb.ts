import { CdkDragDrop, moveItemInArray, transferArrayItem } from '@angular/cdk/drag-drop';
import { Component, OnInit, OnDestroy } from '@angular/core';
import { FormControl, FormGroup, Validators, FormBuilder, FormArray } from '@angular/forms';
import { AnnoqMenuService } from '@annoq.common/services/annoq-menu.service';
import { Subject, Observable } from 'rxjs';
import { startWith, map } from 'rxjs/operators';
import { Annotation } from '../../annotation/models/annotation';
import { AnnotationService } from '../../annotation/services/annotation.service';
import { SnpService } from '../services/snp.service';

@Component({
  selector: 'annoq-annotation-filters',
  templateUrl: './annotation-filters.component.html',
  styleUrls: ['./annotation-filters.component.scss']
})

export class AnnotationFiltersComponent implements OnInit, OnDestroy {
  fieldsFilterForm: FormGroup;
  filteredFields: Observable<any[]>;

  weeks = [];
  connectedTo = [];

  myForm: FormGroup;

  private _unsubscribeAll: Subject<any>;

  indata = {
    companies: [
      {
        projects: [
          {
            projectName: "WB:145787",
          }
        ]
      }
    ]
  }


  filteredAnnotations: Observable<string[]>;
  annotations: Annotation[];



  constructor(
    private fb: FormBuilder,
    public annoqMenuService: AnnoqMenuService,
    public snpService: SnpService,
    private annotationService: AnnotationService
  ) {
    this._unsubscribeAll = new Subject();

    this.myForm = this.fb.group({
      companies: this.fb.array([])
    });


    this.weeks = [
      {
        id: 'week-1',
        weeklist: [
          "item 1",
          "item 2",
          "item 3",
          "item 4",
          "item 5"
        ]
      }, {
        id: 'week-2',
        weeklist: [
          "item 1",
          "item 2",
          "item 3",
          "item 4",
          "item 5"
        ]
      }
    ];
    for (let week of this.weeks) {
      this.connectedTo.push(week.id);
    };
  }


  ngOnInit(): void {
    this.annotations = this.annotationService.annotations.filter((annotation: Annotation) => {
      return annotation.leaf;
    });
    this.fieldsFilterForm = this._createEvidenceDBForm();
  }

  clearValues() {

  }

  addNewCompany() {
    let control = <FormArray>this.myForm.controls.companies;
    control.push(
      this.fb.group({
        company: [''],
        projects: this.fb.array([])
      })
    )
  }

  deleteCompany(index) {
    let control = <FormArray>this.myForm.controls.companies;
    control.removeAt(index)
  }

  addNewProject(control, value?) {
    const projectName = new FormControl(value);
    const projectValue = new FormControl(value);
    control.push(this.fb.group({
      projectName: projectName,
      projectValue: projectValue
    }));

    this._onValueChanges(projectName)
  }

  deleteProject(control, index) {
    control.removeAt(index)
  }

  setCompanies() {
    let control = <FormArray>this.myForm.controls.companies;
    this.indata.companies.forEach(x => {
      control.push(this.fb.group({
        projects: this.setProjects(x)
      }));
    })
  }

  setProjects(x) {
    let arr = new FormArray([]);
    x.projects.forEach(y => {
      this.addNewProject(arr, y.projectName);
    });
    return arr;
  }

  drop(event: CdkDragDrop<string[]>) {
    if (event.previousContainer === event.container) {
      moveItemInArray(event.container.data, event.previousIndex, event.currentIndex);
    } else {
      transferArrayItem(event.previousContainer.data,
        event.container.data,
        event.previousIndex,
        event.currentIndex);
    }
  }

  save() {
    const self = this;
    const errors = [];
    let canSave = true;

    const withs = this.myForm.value.companies.map((project) => {
      return project.projects.map((item) => {
        if (!item.projectName.includes(':')) {
        }
        return item.projectName;
      }).join('|');
    }).join(',');

    if (canSave) {
      console.log(withs);
    } else {
      // self.annoqFormDialogService.openActivityErrorsDialog(errors);
    }
  }

  cancelEvidenceDb() {
    this.fieldsFilterForm.controls['accession'].setValue('');
  }

  private _createEvidenceDBForm() {
    return new FormGroup({
      db: new FormControl(),
      accession: new FormControl('',
        [
          Validators.required,
        ])
    });
  }

  filterFields(value: string): any[] {
    const filterValue = value.toLowerCase();

    return this.annotations.filter((field: Annotation) => field.name.toLowerCase().indexOf(filterValue) === 0);
  }

  private _onValueChanges(formControl: FormControl) {
    const self = this;

    this.filteredFields = formControl.valueChanges
      .pipe(
        startWith(''),
        map(
          value => typeof value === 'string' ? value : value['name']),
        map(field => field ? this.filterFields(field) : this.annotations.slice())
      );

  }

  ngOnDestroy(): void {
    this._unsubscribeAll.next();
    this._unsubscribeAll.complete();
  }
}