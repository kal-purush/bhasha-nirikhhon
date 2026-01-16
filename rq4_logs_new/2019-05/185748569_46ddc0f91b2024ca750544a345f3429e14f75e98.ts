import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AddNewBadgeComponent } from './add-new-badge.component';

describe('AddNewBadgeComponent', () => {
  let component: AddNewBadgeComponent;
  let fixture: ComponentFixture<AddNewBadgeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AddNewBadgeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AddNewBadgeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});