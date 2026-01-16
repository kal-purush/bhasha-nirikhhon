import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BidItemEditComponent } from './bid-item-edit.component';

describe('BidItemEditComponent', () => {
  let component: BidItemEditComponent;
  let fixture: ComponentFixture<BidItemEditComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ BidItemEditComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(BidItemEditComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});