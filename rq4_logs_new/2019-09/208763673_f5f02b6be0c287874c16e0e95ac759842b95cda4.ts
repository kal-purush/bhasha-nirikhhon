import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ManuallocationComponent } from './manuallocation.component';

describe('ManuallocationComponent', () => {
  let component: ManuallocationComponent;
  let fixture: ComponentFixture<ManuallocationComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ManuallocationComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ManuallocationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});