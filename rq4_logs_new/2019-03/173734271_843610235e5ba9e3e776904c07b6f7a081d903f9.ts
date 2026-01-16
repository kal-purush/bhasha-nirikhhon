import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GoodDeedsDetailsComponent } from './good-deeds-details.component';

describe('GoodDeedsDetailsComponent', () => {
  let component: GoodDeedsDetailsComponent;
  let fixture: ComponentFixture<GoodDeedsDetailsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GoodDeedsDetailsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GoodDeedsDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});