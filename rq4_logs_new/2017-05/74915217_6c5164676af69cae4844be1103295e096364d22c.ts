import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DemoProgressbarComponent } from './demo-progressbar.component';

describe('DemoProgressbarComponent', () => {
  let component: DemoProgressbarComponent;
  let fixture: ComponentFixture<DemoProgressbarComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DemoProgressbarComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DemoProgressbarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});