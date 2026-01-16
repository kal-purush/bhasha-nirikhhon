import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ImagineGameComponent } from './imagine-game.component';

describe('ImagineGameComponent', () => {
  let component: ImagineGameComponent;
  let fixture: ComponentFixture<ImagineGameComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ImagineGameComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ImagineGameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});