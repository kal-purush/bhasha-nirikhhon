import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ServicesIntroComponent } from './services-intro.component';

describe('ServicesIntroComponent', () => {
  let component: ServicesIntroComponent;
  let fixture: ComponentFixture<ServicesIntroComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ServicesIntroComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ServicesIntroComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});