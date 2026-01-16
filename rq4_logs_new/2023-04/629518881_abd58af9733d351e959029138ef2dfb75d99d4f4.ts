import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ScrollItemsComponent } from './scroll-items.component';

describe('ScrollItemsComponent', () => {
  let component: ScrollItemsComponent;
  let fixture: ComponentFixture<ScrollItemsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ScrollItemsComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ScrollItemsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});