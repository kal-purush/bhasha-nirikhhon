import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FeditArtistComponent } from './fedit-artist.component';

describe('FeditArtistComponent', () => {
  let component: FeditArtistComponent;
  let fixture: ComponentFixture<FeditArtistComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FeditArtistComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FeditArtistComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});