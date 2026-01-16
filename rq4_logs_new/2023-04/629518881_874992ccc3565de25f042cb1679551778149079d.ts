import { ComponentFixture, TestBed } from '@angular/core/testing';

import { LadingPageNavbarComponent } from './lading-page-navbar.component';

describe('LadingPageNavbarComponent', () => {
  let component: LadingPageNavbarComponent;
  let fixture: ComponentFixture<LadingPageNavbarComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ LadingPageNavbarComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(LadingPageNavbarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});