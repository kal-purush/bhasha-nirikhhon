import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PlayModeSelectComponent } from './play-mode-select.component';

describe('PlayModeSelectComponent', () => {
  let component: PlayModeSelectComponent;
  let fixture: ComponentFixture<PlayModeSelectComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [PlayModeSelectComponent]
    });
    fixture = TestBed.createComponent(PlayModeSelectComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});