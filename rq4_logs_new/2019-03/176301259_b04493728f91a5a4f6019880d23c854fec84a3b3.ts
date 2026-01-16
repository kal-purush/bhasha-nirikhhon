import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ActiveGameListComponent } from './active-game-list.component';

describe('ActiveGameListComponent', () => {
  let component: ActiveGameListComponent;
  let fixture: ComponentFixture<ActiveGameListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ActiveGameListComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ActiveGameListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});