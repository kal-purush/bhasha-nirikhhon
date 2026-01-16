import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SwitchPlayerModalComponent } from './switch-player-modal.component';

describe('SwitchPlayerModalComponent', () => {
  let component: SwitchPlayerModalComponent;
  let fixture: ComponentFixture<SwitchPlayerModalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SwitchPlayerModalComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SwitchPlayerModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});