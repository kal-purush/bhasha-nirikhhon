import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AdExtendComponent } from './ad-extend.component';

describe('AdExtendComponent', () => {
  let component: AdExtendComponent;
  let fixture: ComponentFixture<AdExtendComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AdExtendComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdExtendComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});