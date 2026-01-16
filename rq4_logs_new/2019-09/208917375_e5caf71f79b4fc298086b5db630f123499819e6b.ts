import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GroupcreationformComponent } from './groupcreationform.component';

describe('GroupcreationformComponent', () => {
  let component: GroupcreationformComponent;
  let fixture: ComponentFixture<GroupcreationformComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GroupcreationformComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GroupcreationformComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});