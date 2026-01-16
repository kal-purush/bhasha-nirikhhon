import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EasyTabComponent } from './easy-tab.component';

describe('EasyTabComponent', () => {
  let component: EasyTabComponent;
  let fixture: ComponentFixture<EasyTabComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EasyTabComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EasyTabComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});