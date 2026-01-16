import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ReservecarComponent } from './reservecar.component';

describe('ReservecarComponent', () => {
  let component: ReservecarComponent;
  let fixture: ComponentFixture<ReservecarComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ReservecarComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ReservecarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});