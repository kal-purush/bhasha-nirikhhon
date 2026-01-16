import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TodosLosReclamosComponent } from './todos-los-reclamos.component';

describe('TodosLosReclamosComponent', () => {
  let component: TodosLosReclamosComponent;
  let fixture: ComponentFixture<TodosLosReclamosComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TodosLosReclamosComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TodosLosReclamosComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});