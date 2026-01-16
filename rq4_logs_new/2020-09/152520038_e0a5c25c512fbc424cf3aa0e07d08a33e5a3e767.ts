import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PicksLoaderComponent } from './picks-loader.component';

describe('PicksLoaderComponent', () => {
  let component: PicksLoaderComponent;
  let fixture: ComponentFixture<PicksLoaderComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PicksLoaderComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PicksLoaderComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});