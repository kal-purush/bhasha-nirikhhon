import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GoogleMapLargeComponent } from './google-map-large.component';

describe('GoogleMapLargeComponent', () => {
  let component: GoogleMapLargeComponent;
  let fixture: ComponentFixture<GoogleMapLargeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [GoogleMapLargeComponent],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GoogleMapLargeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});