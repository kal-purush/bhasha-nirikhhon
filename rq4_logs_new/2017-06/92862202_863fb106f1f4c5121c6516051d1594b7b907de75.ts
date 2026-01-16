import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PlayerTechnologyComponent } from './player-technology.component';

describe('PlayerTechnologyComponent', () => {
  let component: PlayerTechnologyComponent;
  let fixture: ComponentFixture<PlayerTechnologyComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PlayerTechnologyComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PlayerTechnologyComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});