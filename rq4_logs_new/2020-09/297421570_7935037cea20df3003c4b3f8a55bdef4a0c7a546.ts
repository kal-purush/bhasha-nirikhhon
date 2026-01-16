import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UpgradeButtonComponent } from './upgrade-button.component';

describe('UpgradeButtonComponent', () => {
  let component: UpgradeButtonComponent;
  let fixture: ComponentFixture<UpgradeButtonComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ UpgradeButtonComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UpgradeButtonComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});