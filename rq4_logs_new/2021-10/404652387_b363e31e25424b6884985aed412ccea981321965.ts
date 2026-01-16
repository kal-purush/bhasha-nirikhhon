import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CreateCandidatureComponent } from './create-candidature.component';

describe('CreateCandidatureComponent', () => {
  let component: CreateCandidatureComponent;
  let fixture: ComponentFixture<CreateCandidatureComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CreateCandidatureComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateCandidatureComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});