import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AppliedStudenListComponent } from './applied-studen-list.component';

describe('AppliedStudenListComponent', () => {
  let component: AppliedStudenListComponent;
  let fixture: ComponentFixture<AppliedStudenListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AppliedStudenListComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(AppliedStudenListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});