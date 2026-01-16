import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ComponentGalleryComponent } from './component-gallery.component';

describe('ComponentGalleryComponent', () => {
  let component: ComponentGalleryComponent;
  let fixture: ComponentFixture<ComponentGalleryComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ComponentGalleryComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ComponentGalleryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});