import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BodyGalleryComponent } from './body-gallery.component';

describe('BodyGalleryComponent', () => {
  let component: BodyGalleryComponent;
  let fixture: ComponentFixture<BodyGalleryComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ BodyGalleryComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(BodyGalleryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});