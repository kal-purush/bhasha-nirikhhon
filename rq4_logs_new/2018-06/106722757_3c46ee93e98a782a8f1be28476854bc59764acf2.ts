import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EditplanclientComponent } from './editplanclient.component';

describe('EditplanclientComponent', () => {
  let component: EditplanclientComponent;
  let fixture: ComponentFixture<EditplanclientComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EditplanclientComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EditplanclientComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});