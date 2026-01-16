import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DataGraphsComponent } from './data-graphs.component';

describe('DataGraphsComponent', () => {
  let component: DataGraphsComponent;
  let fixture: ComponentFixture<DataGraphsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DataGraphsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DataGraphsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});