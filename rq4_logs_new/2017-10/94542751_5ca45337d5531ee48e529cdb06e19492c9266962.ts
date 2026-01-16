import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ZoneEditComponent } from './zone-edit.component';

describe('ZoneEditComponent', () => {
  let component: ZoneEditComponent;
  let fixture: ComponentFixture<ZoneEditComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ZoneEditComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ZoneEditComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});