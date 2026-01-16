import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BottomKeyComponent } from './bottom-key.component';

describe('BottomKeyComponent', () => {
  let component: BottomKeyComponent;
  let fixture: ComponentFixture<BottomKeyComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BottomKeyComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BottomKeyComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});