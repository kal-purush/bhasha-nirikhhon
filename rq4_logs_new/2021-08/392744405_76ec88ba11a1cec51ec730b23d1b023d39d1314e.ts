import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HackCompleteComponent } from './hack-complete.component';

describe('HackCompleteComponent', () => {
  let component: HackCompleteComponent;
  let fixture: ComponentFixture<HackCompleteComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ HackCompleteComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HackCompleteComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});