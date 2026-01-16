import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ManageOpportunitiesComponent } from './manage-opportunities.component';

describe('ManageOpportunitiesComponent', () => {
  let component: ManageOpportunitiesComponent;
  let fixture: ComponentFixture<ManageOpportunitiesComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ManageOpportunitiesComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ManageOpportunitiesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});