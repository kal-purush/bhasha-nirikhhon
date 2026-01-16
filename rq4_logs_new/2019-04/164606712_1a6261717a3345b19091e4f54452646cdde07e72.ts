import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { DepartmentGroupComponent } from './department-group.component';
import { GroupService } from 'src/app/admin/modules/permission-management/services/group.service';
import { Department } from 'src/app/shared/interfaces/department';
import { Subject } from 'rxjs';

describe('DepartmentGroupComponent', () => {
  let component: DepartmentGroupComponent;
  let fixture: ComponentFixture<DepartmentGroupComponent>;
  let getGroupByDepartmentName$: Subject<{}>;

  beforeEach(async(() => {
    getGroupByDepartmentName$ = new Subject();
    TestBed.configureTestingModule({
      declarations: [ DepartmentGroupComponent ],
      providers: [
        {
          provide: GroupService,
          useValue: {
            getGroupByDepartmentName: () => getGroupByDepartmentName$,
          },
        },
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DepartmentGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should get groups', () =>{
    const department = {id: 1, name: 'name'} as Department;
    getGroupByDepartmentName$.next(department);
  })
});