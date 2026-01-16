import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CalendrierHeaderComponent } from './calendrier-header.component';

describe('CalendrierHeaderComponent', () => {
  let component: CalendrierHeaderComponent;
  let fixture: ComponentFixture<CalendrierHeaderComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CalendrierHeaderComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CalendrierHeaderComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});