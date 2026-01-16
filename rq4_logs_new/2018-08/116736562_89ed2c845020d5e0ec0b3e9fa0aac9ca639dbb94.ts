import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DesignClarioDimensionsComponent } from './design-clario-dimensions.component';

describe('DesignClarioDimensionsComponent', () => {
  let component: DesignClarioDimensionsComponent;
  let fixture: ComponentFixture<DesignClarioDimensionsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DesignClarioDimensionsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DesignClarioDimensionsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});