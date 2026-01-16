import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GrassrootsAboutComponent } from './grassroots-about.component';

describe('GrassrootsAboutComponent', () => {
  let component: GrassrootsAboutComponent;
  let fixture: ComponentFixture<GrassrootsAboutComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GrassrootsAboutComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GrassrootsAboutComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});