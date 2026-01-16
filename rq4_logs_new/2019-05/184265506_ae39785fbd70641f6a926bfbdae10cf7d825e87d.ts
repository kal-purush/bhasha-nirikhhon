import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GgithabButtonComponent } from './ggithab-button.component';

describe('GgithabButtonComponent', () => {
  let component: GgithabButtonComponent;
  let fixture: ComponentFixture<GgithabButtonComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GgithabButtonComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GgithabButtonComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});