import { ComponentFixture, TestBed } from '@angular/core/testing';

import { StudentHistoryPageComponent } from './student-history-page.component';

describe('StudentHistoryPageComponent', () => {
  let component: StudentHistoryPageComponent;
  let fixture: ComponentFixture<StudentHistoryPageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ StudentHistoryPageComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(StudentHistoryPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});