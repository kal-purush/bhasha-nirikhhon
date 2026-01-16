import {async, ComponentFixture, TestBed} from '@angular/core/testing';

import {CameraviewComponent} from './cameraview.component';
import {BrowserModule} from '@angular/platform-browser';
import {FormsModule} from '@angular/forms';
import {NgbModule} from '@ng-bootstrap/ng-bootstrap';
import {ThreejsComponent} from '../threejs/threejs.component';
import {VideoComponent} from '../video/video.component';

describe('CameraviewComponent', () => {
  let component: CameraviewComponent;
  let fixture: ComponentFixture<CameraviewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [
        CameraviewComponent,
        ThreejsComponent,
        VideoComponent
      ],

      imports: [
        BrowserModule,
        FormsModule,
        NgbModule
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CameraviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});