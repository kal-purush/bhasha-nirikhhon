import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ConnectedLibrariesComponent } from './connected-libraries.component';

describe('ConnectedLibrariesComponent', () => {
  let component: ConnectedLibrariesComponent;
  let fixture: ComponentFixture<ConnectedLibrariesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ConnectedLibrariesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ConnectedLibrariesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});