import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ViewListOfCollectiblesComponent } from './view-list-of-collectibles.component';

describe('ViewListOfCollectiblesComponent', () => {
  let component: ViewListOfCollectiblesComponent;
  let fixture: ComponentFixture<ViewListOfCollectiblesComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ViewListOfCollectiblesComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ViewListOfCollectiblesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});