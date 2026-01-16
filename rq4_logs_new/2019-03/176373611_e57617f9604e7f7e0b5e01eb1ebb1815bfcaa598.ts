import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CityThumbnailComponent } from './city-thumbnail.component';

describe('CityThumbnailComponent', () => {
  let component: CityThumbnailComponent;
  let fixture: ComponentFixture<CityThumbnailComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CityThumbnailComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CityThumbnailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});