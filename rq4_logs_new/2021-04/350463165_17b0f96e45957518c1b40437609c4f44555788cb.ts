import { ComponentFixture, TestBed } from '@angular/core/testing';

import { StudentsTablePageComponent } from './students-table-page.component';

describe('StudentsTablePageComponent', () => {
  let component: StudentsTablePageComponent;
  let fixture: ComponentFixture<StudentsTablePageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ StudentsTablePageComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(StudentsTablePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});