import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DeckPageComponent } from './deck-page.component';

describe('DeckPageComponent', () => {
  let component: DeckPageComponent;
  let fixture: ComponentFixture<DeckPageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [DeckPageComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DeckPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});