import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AsaquaComponent } from './asaqua.component';

describe('AsaquaComponent', () => {
  let component: AsaquaComponent;
  let fixture: ComponentFixture<AsaquaComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AsaquaComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AsaquaComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});