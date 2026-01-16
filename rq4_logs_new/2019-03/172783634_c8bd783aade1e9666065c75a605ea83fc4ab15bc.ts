import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PrefixerComponent } from './prefixer.component';

describe('PrefixerComponent', () => {
  let component: PrefixerComponent;
  let fixture: ComponentFixture<PrefixerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PrefixerComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PrefixerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});