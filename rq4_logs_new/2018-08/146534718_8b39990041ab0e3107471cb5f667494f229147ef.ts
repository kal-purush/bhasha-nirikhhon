import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FpaComponent } from './fpa.component';

describe('FpaComponent', () => {
  let component: FpaComponent;
  let fixture: ComponentFixture<FpaComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FpaComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FpaComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});