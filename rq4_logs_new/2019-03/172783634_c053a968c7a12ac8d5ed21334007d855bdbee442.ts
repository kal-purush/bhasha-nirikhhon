import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TjsSubtractComponent } from './tjs-subtract.component';

describe('TjsSubtractComponent', () => {
  let component: TjsSubtractComponent;
  let fixture: ComponentFixture<TjsSubtractComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TjsSubtractComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TjsSubtractComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});