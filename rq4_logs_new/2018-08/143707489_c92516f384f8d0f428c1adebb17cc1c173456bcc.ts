import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { RxjsPipeComponent } from './rxjs-pipe.component';

describe('RxjsPipeComponent', () => {
  let component: RxjsPipeComponent;
  let fixture: ComponentFixture<RxjsPipeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RxjsPipeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RxjsPipeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});