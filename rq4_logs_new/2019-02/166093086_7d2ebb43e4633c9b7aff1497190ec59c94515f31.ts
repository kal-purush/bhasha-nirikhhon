import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LoomisComponent } from './loomis.component';

describe('LoomisComponent', () => {
  let component: LoomisComponent;
  let fixture: ComponentFixture<LoomisComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LoomisComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LoomisComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});