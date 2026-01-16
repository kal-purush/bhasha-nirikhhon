import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TjsAdditionComponent } from './tjs-addition.component';

describe('TjsAdditionComponent', () => {
  let component: TjsAdditionComponent;
  let fixture: ComponentFixture<TjsAdditionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TjsAdditionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TjsAdditionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});