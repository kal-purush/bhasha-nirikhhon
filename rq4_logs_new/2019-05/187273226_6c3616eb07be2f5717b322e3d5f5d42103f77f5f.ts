import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AddTileComponent } from './add-tile.component';

describe('AddTileComponent', () => {
  let component: AddTileComponent;
  let fixture: ComponentFixture<AddTileComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AddTileComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AddTileComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});