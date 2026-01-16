import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { StartGameModalComponent } from './start-game-modal.component';

describe('StartGameModalComponent', () => {
  let component: StartGameModalComponent;
  let fixture: ComponentFixture<StartGameModalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ StartGameModalComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(StartGameModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});