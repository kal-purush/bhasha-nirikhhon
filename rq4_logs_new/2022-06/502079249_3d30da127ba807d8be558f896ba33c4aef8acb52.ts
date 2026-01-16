import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HowIAmDoingComponent } from './how-i-am-doing.component';

describe('HowIAmDoingComponent', () => {
  let component: HowIAmDoingComponent;
  let fixture: ComponentFixture<HowIAmDoingComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ HowIAmDoingComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HowIAmDoingComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});