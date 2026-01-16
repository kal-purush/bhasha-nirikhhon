import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ImagepopupComponent } from './imagepopup.component';

describe('ImagepopupComponent', () => {
  let component: ImagepopupComponent;
  let fixture: ComponentFixture<ImagepopupComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ImagepopupComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ImagepopupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});