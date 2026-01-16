import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { WebcamBoxComponent } from './webcam-box.component';

describe('WebcamBoxComponent', () => {
  let component: WebcamBoxComponent;
  let fixture: ComponentFixture<WebcamBoxComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ WebcamBoxComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(WebcamBoxComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});