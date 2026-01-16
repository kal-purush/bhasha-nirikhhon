import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FkNewsCardComponent } from './fk-news-card.component';

describe('FkNewsCardComponent', () => {
  let component: FkNewsCardComponent;
  let fixture: ComponentFixture<FkNewsCardComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FkNewsCardComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FkNewsCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});