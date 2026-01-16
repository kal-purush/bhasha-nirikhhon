import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HappybirthdayComponent } from './happybirthday.component';

describe('HappybirthdayComponent', () => {
  let component: HappybirthdayComponent;
  let fixture: ComponentFixture<HappybirthdayComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ HappybirthdayComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(HappybirthdayComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});