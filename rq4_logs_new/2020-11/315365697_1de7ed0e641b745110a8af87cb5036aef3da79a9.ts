import { ComponentFixture, TestBed } from '@angular/core/testing';

import { IndividualPanelComponent } from './individual-panel.component';

describe('IndivdualPanelComponent', () => {
  let component: IndividualPanelComponent;
  let fixture: ComponentFixture<IndividualPanelComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ IndividualPanelComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IndividualPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});