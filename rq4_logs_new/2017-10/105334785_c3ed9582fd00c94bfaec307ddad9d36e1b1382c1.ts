import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DataVisulizationComponent } from './data-visulization.component';

describe('DataVisulizationComponent', () => {
  let component: DataVisulizationComponent;
  let fixture: ComponentFixture<DataVisulizationComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DataVisulizationComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DataVisulizationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});