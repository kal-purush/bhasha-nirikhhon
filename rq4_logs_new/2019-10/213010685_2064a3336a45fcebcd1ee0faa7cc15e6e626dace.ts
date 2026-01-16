import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PixbaypostsComponent } from './pixbayposts.component';

describe('PixbaypostsComponent', () => {
  let component: PixbaypostsComponent;
  let fixture: ComponentFixture<PixbaypostsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PixbaypostsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PixbaypostsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});