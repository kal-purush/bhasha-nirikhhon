import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ConnectToContactsComponent } from './connect-to-contacts.component';

describe('ConnectToContactsComponent', () => {
  let component: ConnectToContactsComponent;
  let fixture: ComponentFixture<ConnectToContactsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ConnectToContactsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ConnectToContactsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});