import { ComponentFixture, TestBed } from '@angular/core/testing';

import { StudentMainPageComponent } from './student-main-page.component';

describe('StudentMainPageComponent', () => {
  let component: StudentMainPageComponent;
  let fixture: ComponentFixture<StudentMainPageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [StudentMainPageComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(StudentMainPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});