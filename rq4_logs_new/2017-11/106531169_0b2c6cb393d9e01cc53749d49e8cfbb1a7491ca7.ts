import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ComponentRootComponent } from './component-root.component';

describe('ComponentRootComponent', () => {
  let component: ComponentRootComponent;
  let fixture: ComponentFixture<ComponentRootComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ComponentRootComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ComponentRootComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});