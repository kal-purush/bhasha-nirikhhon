import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AdvertisementGridComponent } from './advertisement-grid.component';

describe('AdvertisementGridComponent', () => {
  let component: AdvertisementGridComponent;
  let fixture: ComponentFixture<AdvertisementGridComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AdvertisementGridComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AdvertisementGridComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});