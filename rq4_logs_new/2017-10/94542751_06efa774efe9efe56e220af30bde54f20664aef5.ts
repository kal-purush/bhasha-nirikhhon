import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BrokerEditComponent } from './broker-edit.component';

describe('BrokerEditComponent', () => {
  let component: BrokerEditComponent;
  let fixture: ComponentFixture<BrokerEditComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BrokerEditComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BrokerEditComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});