import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { IppoComponent } from './ippo.component';

describe('IppoComponent', () => {
  let component: IppoComponent;
  let fixture: ComponentFixture<IppoComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ IppoComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(IppoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});