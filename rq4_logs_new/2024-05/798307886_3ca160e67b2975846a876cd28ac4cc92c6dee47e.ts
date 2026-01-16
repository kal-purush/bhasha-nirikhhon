import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ListDatesComponent } from './list-dates.component';

describe('ListDatesComponent', () => {
  let component: ListDatesComponent;
  let fixture: ComponentFixture<ListDatesComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ListDatesComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(ListDatesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});