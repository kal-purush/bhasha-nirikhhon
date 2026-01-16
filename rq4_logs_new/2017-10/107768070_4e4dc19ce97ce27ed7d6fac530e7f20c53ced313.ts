import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ImportantNoUrgentItemsComponent } from './important-no-urgent-items.component';

describe('ImportantNoUrgentItemsComponent', () => {
  let component: ImportantNoUrgentItemsComponent;
  let fixture: ComponentFixture<ImportantNoUrgentItemsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ImportantNoUrgentItemsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ImportantNoUrgentItemsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});