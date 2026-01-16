import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BuyResourceComponent } from './buy-resource.component';

describe('BuyResourceComponent', () => {
  let component: BuyResourceComponent;
  let fixture: ComponentFixture<BuyResourceComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BuyResourceComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BuyResourceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});