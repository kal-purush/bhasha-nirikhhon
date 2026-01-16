import { ComponentFixture, TestBed } from '@angular/core/testing';

import { InfoSectionHeaderComponent } from './info-section-header.component';

describe('InfoSectionHeaderComponent', () => {
  let component: InfoSectionHeaderComponent;
  let fixture: ComponentFixture<InfoSectionHeaderComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ InfoSectionHeaderComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(InfoSectionHeaderComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});