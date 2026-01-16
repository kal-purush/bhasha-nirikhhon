import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FilepicComponent } from './filepic.component';

describe('FilepicComponent', () => {
  let component: FilepicComponent;
  let fixture: ComponentFixture<FilepicComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FilepicComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FilepicComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});