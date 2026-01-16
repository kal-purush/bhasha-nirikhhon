import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DangerIndicatorComponent } from './danger-indicator.component';

describe('DangerIndicatorComponent', () => {
  let component: DangerIndicatorComponent;
  let fixture: ComponentFixture<DangerIndicatorComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DangerIndicatorComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DangerIndicatorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});