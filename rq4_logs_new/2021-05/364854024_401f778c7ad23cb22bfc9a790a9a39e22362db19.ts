import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ArtistsFormUpdateComponent } from './artists-form-update.component';

describe('ArtistsFormUpdateComponent', () => {
  let component: ArtistsFormUpdateComponent;
  let fixture: ComponentFixture<ArtistsFormUpdateComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ArtistsFormUpdateComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ArtistsFormUpdateComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});