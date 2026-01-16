import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CollpasiblePanelComponent } from './collapsible-panel.component';

describe('CollpasiblePanelComponent', () => {
  let component: CollpasiblePanelComponent;
  let fixture: ComponentFixture<CollpasiblePanelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CollpasiblePanelComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CollpasiblePanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});