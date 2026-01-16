import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AssociationInfoComponent } from './association-info.component';

describe('AssociationInfoComponent', () => {
  let component: AssociationInfoComponent;
  let fixture: ComponentFixture<AssociationInfoComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [AssociationInfoComponent]
    });
    fixture = TestBed.createComponent(AssociationInfoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});