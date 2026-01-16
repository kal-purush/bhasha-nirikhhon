import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TorneosVerComponent } from './torneos-ver.component';

describe('TorneosVerComponent', () => {
  let component: TorneosVerComponent;
  let fixture: ComponentFixture<TorneosVerComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TorneosVerComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TorneosVerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});