import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AsymencryptionComponent } from './asymencryption.component';

describe('AsymencryptionComponent', () => {
  let component: AsymencryptionComponent;
  let fixture: ComponentFixture<AsymencryptionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AsymencryptionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AsymencryptionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});