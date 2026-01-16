import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HistoryPostComponent } from './history-post.component';

describe('HistoryPostComponent', () => {
  let component: HistoryPostComponent;
  let fixture: ComponentFixture<HistoryPostComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ HistoryPostComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HistoryPostComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});