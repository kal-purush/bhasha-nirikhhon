import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DocsButtonsComponent } from './docs-buttons.component';

describe('DocsButtonsComponent', () => {
  let component: DocsButtonsComponent;
  let fixture: ComponentFixture<DocsButtonsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DocsButtonsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DocsButtonsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});