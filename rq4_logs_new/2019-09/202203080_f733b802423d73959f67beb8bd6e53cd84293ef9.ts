import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FetchTestimoniesComponent } from './fetch-testimonies.component';

describe('FetchTestimoniesComponent', () => {
  let component: FetchTestimoniesComponent;
  let fixture: ComponentFixture<FetchTestimoniesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FetchTestimoniesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FetchTestimoniesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});