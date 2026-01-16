import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AbcquestionComponent } from './abcquestion.component';

describe('AbcquestionComponent', () => {
  let component: AbcquestionComponent;
  let fixture: ComponentFixture<AbcquestionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [AbcquestionComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(AbcquestionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});