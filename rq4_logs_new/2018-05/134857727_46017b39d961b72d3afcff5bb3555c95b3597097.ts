import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EditPartyComponent } from './edit-party.component';

describe('EditPartyComponent', () => {
  let component: EditPartyComponent;
  let fixture: ComponentFixture<EditPartyComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EditPartyComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EditPartyComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});