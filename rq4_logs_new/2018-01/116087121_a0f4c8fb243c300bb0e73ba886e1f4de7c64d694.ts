import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PirceComponent } from './pirce.component';

describe('PirceComponent', () => {
  let component: PirceComponent;
  let fixture: ComponentFixture<PirceComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PirceComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PirceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});