import { ComponentFixture, TestBed } from '@angular/core/testing';

import { IndustryProfileComponent } from './industry-profile.component';

describe('IndustryProfileComponent', () => {
  let component: IndustryProfileComponent;
  let fixture: ComponentFixture<IndustryProfileComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ IndustryProfileComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(IndustryProfileComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});