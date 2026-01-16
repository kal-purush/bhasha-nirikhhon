import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CauroselComponent } from './caurosel.component';

describe('CauroselComponent', () => {
  let component: CauroselComponent;
  let fixture: ComponentFixture<CauroselComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CauroselComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CauroselComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});