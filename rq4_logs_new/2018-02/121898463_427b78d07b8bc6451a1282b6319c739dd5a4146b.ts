import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CreatorFormComponent } from './creator-form.component';

describe('CreatorFormComponent', () => {
  let component: CreatorFormComponent;
  let fixture: ComponentFixture<CreatorFormComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CreatorFormComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CreatorFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});