import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CoffeeSavingsComponent } from './coffee-savings.component';

describe('CoffeeSavingsComponent', () => {
  let component: CoffeeSavingsComponent;
  let fixture: ComponentFixture<CoffeeSavingsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CoffeeSavingsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CoffeeSavingsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});