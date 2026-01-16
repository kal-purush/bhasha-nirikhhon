import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ConfigureEventComponent } from './configure-event.component';

describe('ConfigureEventComponent', () => {
  let component: ConfigureEventComponent;
  let fixture: ComponentFixture<ConfigureEventComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ConfigureEventComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigureEventComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});