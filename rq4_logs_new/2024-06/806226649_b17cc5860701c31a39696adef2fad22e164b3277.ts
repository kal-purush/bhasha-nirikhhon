import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DateSelectComponent } from './date-select.component';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

describe('DateSelectComponent', () => {
  let component: DateSelectComponent;
  let fixture: ComponentFixture<DateSelectComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [DateSelectComponent, NoopAnimationsModule]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(DateSelectComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});