import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LibCreateComponent } from './lib-create.component';

describe('LibCreateComponent', () => {
  let component: LibCreateComponent;
  let fixture: ComponentFixture<LibCreateComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LibCreateComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LibCreateComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});