import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CasinoComponentComponent } from './casino-component.component';

describe('CasinoComponentComponent', () => {
  let component: CasinoComponentComponent;
  let fixture: ComponentFixture<CasinoComponentComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CasinoComponentComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CasinoComponentComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});