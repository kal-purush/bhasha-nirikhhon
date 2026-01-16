import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LightbarComponent } from './lightbar.component';

describe('LightbarComponent', () => {
  let component: LightbarComponent;
  let fixture: ComponentFixture<LightbarComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LightbarComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LightbarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});