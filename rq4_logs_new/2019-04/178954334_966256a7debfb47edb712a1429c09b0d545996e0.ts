import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MostarCarritoComponent } from './mostar-carrito.component';

describe('MostarCarritoComponent', () => {
  let component: MostarCarritoComponent;
  let fixture: ComponentFixture<MostarCarritoComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MostarCarritoComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MostarCarritoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});