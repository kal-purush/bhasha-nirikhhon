import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ThematicViewComponent } from './thematic-view.component';

describe('ThematicViewComponent', () => {
  let component: ThematicViewComponent;
  let fixture: ComponentFixture<ThematicViewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ThematicViewComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ThematicViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});