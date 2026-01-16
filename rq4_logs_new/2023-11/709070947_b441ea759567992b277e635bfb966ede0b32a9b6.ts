import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CustomEndpointComponent } from './custom-endpoint.component';

describe('CustomEndpointComponent', () => {
  let component: CustomEndpointComponent;
  let fixture: ComponentFixture<CustomEndpointComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [CustomEndpointComponent]
    });
    fixture = TestBed.createComponent(CustomEndpointComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});