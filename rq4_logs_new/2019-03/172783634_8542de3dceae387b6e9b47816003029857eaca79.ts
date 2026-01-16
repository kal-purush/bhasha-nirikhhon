import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DfsSubtractComponent } from './dfs-subtract.component';

describe('DfsSubtractComponent', () => {
  let component: DfsSubtractComponent;
  let fixture: ComponentFixture<DfsSubtractComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DfsSubtractComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DfsSubtractComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});