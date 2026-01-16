import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CollectionxQuestsCardComponent } from './collectionx-quests-card.component';

describe('CollectionxQuestsCardComponent', () => {
  let component: CollectionxQuestsCardComponent;
  let fixture: ComponentFixture<CollectionxQuestsCardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CollectionxQuestsCardComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CollectionxQuestsCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});