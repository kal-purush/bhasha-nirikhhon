import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AdduniversityComponent } from './adduniversity.component';

describe('AdduniversityComponent', () => {
  let component: AdduniversityComponent;
  let fixture: ComponentFixture<AdduniversityComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AdduniversityComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdduniversityComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});