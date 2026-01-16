import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GixButtonCornectoComponent } from './gix-button-cornecto.component';

describe('GixButtonCornectoComponent', () => {
  let component: GixButtonCornectoComponent;
  let fixture: ComponentFixture<GixButtonCornectoComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GixButtonCornectoComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GixButtonCornectoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});