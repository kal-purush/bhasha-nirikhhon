import { ComponentFixture, TestBed } from '@angular/core/testing';

import { GahcaTitlesComponent } from './gahca-titles.component';

describe('GahcaTitlesComponent', () => {
  let component: GahcaTitlesComponent;
  let fixture: ComponentFixture<GahcaTitlesComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [GahcaTitlesComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(GahcaTitlesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});