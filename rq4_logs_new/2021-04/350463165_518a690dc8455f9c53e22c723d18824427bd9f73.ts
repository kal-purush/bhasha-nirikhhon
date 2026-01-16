import { ComponentFixture, TestBed } from '@angular/core/testing';

import { InfoStudentPageComponent } from './info-student-page.component';

describe('InfoStudentPageComponent', () => {
  let component: InfoStudentPageComponent;
  let fixture: ComponentFixture<InfoStudentPageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ InfoStudentPageComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(InfoStudentPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});