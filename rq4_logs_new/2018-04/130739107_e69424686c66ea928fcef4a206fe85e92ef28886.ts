import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MyShelfLabelComponent } from './my-shelf-label.component';

describe('MyShelfLabelComponent', () => {
  let component: MyShelfLabelComponent;
  let fixture: ComponentFixture<MyShelfLabelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MyShelfLabelComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MyShelfLabelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});