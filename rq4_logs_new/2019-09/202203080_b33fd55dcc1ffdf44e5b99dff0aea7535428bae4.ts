import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FetchLocationsComponent } from './fetch-locations.component';

describe('FetchLocationsComponent', () => {
  let component: FetchLocationsComponent;
  let fixture: ComponentFixture<FetchLocationsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FetchLocationsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FetchLocationsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});