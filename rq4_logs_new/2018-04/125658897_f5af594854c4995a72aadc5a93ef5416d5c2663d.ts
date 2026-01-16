import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LuminariaComponent } from './luminaria.component';

describe('LuminariaComponent', () => {
  let component: LuminariaComponent;
  let fixture: ComponentFixture<LuminariaComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LuminariaComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LuminariaComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});