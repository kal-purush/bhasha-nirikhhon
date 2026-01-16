import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TanksComponent } from './tanks.component';

describe('TanksComponent', () => {
  let component: TanksComponent;
  let fixture: ComponentFixture<TanksComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TanksComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TanksComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});