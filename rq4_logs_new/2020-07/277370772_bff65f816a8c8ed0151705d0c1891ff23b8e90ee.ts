import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AdvertAddComponent } from './advert-add.component';

describe('AdvertAddComponent', () => {
  let component: AdvertAddComponent;
  let fixture: ComponentFixture<AdvertAddComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AdvertAddComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdvertAddComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});