import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AngularzoneComponent } from './angularzone.component';

describe('AngularzoneComponent', () => {
  let component: AngularzoneComponent;
  let fixture: ComponentFixture<AngularzoneComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AngularzoneComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AngularzoneComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});