import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ZoomInProfilePicComponent } from './zoom-in-profile-pic.component';

describe('ZoomInProfilePicComponent', () => {
  let component: ZoomInProfilePicComponent;
  let fixture: ComponentFixture<ZoomInProfilePicComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ZoomInProfilePicComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ZoomInProfilePicComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});