import { ComponentFixture, TestBed } from '@angular/core/testing';

import { APITestComponent } from './api-test.component';

describe('APITestComponent', () => {
  let component: APITestComponent;
  let fixture: ComponentFixture<APITestComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ APITestComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(APITestComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});