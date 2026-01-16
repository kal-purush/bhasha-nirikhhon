import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ImagenesPoderosasComponent } from './imagenes-poderosas.component';

describe('ImagenesPoderosasComponent', () => {
  let component: ImagenesPoderosasComponent;
  let fixture: ComponentFixture<ImagenesPoderosasComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ImagenesPoderosasComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ImagenesPoderosasComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});