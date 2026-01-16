import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DynamicplayerComponent } from './dynamicplayer.component';

describe('DynamicplayerComponent', () => {
  let component: DynamicplayerComponent;
  let fixture: ComponentFixture<DynamicplayerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DynamicplayerComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DynamicplayerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});