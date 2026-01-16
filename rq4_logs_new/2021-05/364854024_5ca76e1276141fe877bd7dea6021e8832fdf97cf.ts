import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CompleteArtistsFormComponent } from './complete-artists-form.component';

describe('CompleteArtistsFormComponent', () => {
  let component: CompleteArtistsFormComponent;
  let fixture: ComponentFixture<CompleteArtistsFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CompleteArtistsFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CompleteArtistsFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});