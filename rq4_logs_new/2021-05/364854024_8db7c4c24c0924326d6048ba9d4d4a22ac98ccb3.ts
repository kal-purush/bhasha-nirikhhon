import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ArtistDisciplineComponent } from './artist-discipline.component';

describe('ArtistDisciplineComponent', () => {
  let component: ArtistDisciplineComponent;
  let fixture: ComponentFixture<ArtistDisciplineComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ArtistDisciplineComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ArtistDisciplineComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});