import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AutoSearchStartComponent } from './auto-search-start.component';

describe('AutoSearchStartComponent', () => {
  let component: AutoSearchStartComponent;
  let fixture: ComponentFixture<AutoSearchStartComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AutoSearchStartComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AutoSearchStartComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});